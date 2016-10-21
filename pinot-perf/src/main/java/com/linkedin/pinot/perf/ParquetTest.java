/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.FixedBitSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.RLEForwardIndexReader;
import com.linkedin.pinot.core.io.reader.impl.RLEBitForwardIndexReader;
import com.linkedin.pinot.core.io.reader.impl.RLEValueReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.MultiValueReaderContext;
import com.linkedin.pinot.core.segment.creator.impl.fwd.RLEForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.RLEBitForwardIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import me.lemire.integercompression.BitPacking;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import org.apache.http.nio.util.DirectByteBufferAllocator;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;


/**
 * Given a pinot segment directory, it benchmarks forward index scan speed
 */
public class ParquetTest {
  static int MAX_RUNS = 10;

  public static void singleValuedReadBenchMarkV1(File file, int numDocs, int columnSizeInBits, String column)
      throws Exception {

    boolean signed = false;
    boolean isMmap = false;
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "benchmark");
    BaseSingleColumnSingleValueReader reader =
        new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader(heapBuffer, numDocs,
            columnSizeInBits, signed);
    // sequential read
    long start, end;
    final int PAGE_SIZE = 20000;
    final int valsPerPage = 100 ;
    long parquetFileSize = 0;
    String outputFileName = column+ ".rle";
    String outputFileName1 = column + ".data";
    String outputFileName2 = column + ".orig";
    String outputFileName3 = column + ".new";
    FileOutputStream outputFile = new FileOutputStream(outputFileName);
    PrintWriter outputFile1 = new PrintWriter(outputFileName1, "UTF-8");
    PrintWriter outputFile2 = new PrintWriter(outputFileName2, "UTF-8");
    RunLengthBitPackingHybridValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(
        columnSizeInBits, valsPerPage, PAGE_SIZE);
    int indexInPage =0;
    int prevValue = 0;
    int numRuns = 1;
    for (int i = 0; i < numDocs; i++) {
      int v = reader.getInt(i);
      rle.writeInteger(v);
     if (v != prevValue) {
       outputFile1.println(prevValue);
       prevValue = v;
       numRuns++;
    }
      outputFile2.println(v);
      indexInPage++;
    }
    rle.getBytes().writeAllTo(outputFile);
    outputFile.close();
    outputFile1.close();
    outputFile2.close();

    RunLengthBitPackingHybridValuesReader rle_reader = new RunLengthBitPackingHybridValuesReader(columnSizeInBits);
    FileInputStream inputFile = new FileInputStream(outputFileName);
    byte[] in = IOUtils.toByteArray(inputFile);
    rle_reader.initFromPage(numDocs, in, 0);
    PrintWriter outputFile3 = new PrintWriter(outputFileName3, "UTF-8");
    for (int i = 0; i < numDocs; i++) {
      int v = rle_reader.readInteger();
      outputFile3.println(v);
    }
    outputFile3.close();
    FieldSpec spec = new DimensionFieldSpec(column, FieldSpec.DataType.INT, true);

    RLEForwardIndexCreator prle = new RLEForwardIndexCreator(file.getParentFile(),
        columnSizeInBits, spec, numRuns, numDocs);
    RLEBitForwardIndexCreator prle2 =
        new RLEBitForwardIndexCreator(file.getParentFile(), (1<<columnSizeInBits) -1, spec, numRuns, numDocs);
    reader =
        new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader(heapBuffer, numDocs,
            columnSizeInBits, signed);
    for (int i = 0; i < numDocs; i++) {
      int v = reader.getInt(i);
      prle.add(v,i);
      prle2.add(v,i);
    }
    prle.seal();
    prle2.seal();

    File rleFile = new File(file.getParentFile(), column + ".rle1");
    PinotDataBuffer buffer = PinotDataBuffer.fromFile(rleFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "rle1_read");
    FixedByteSingleValueMultiColReader rawFileReader = new FixedByteSingleValueMultiColReader(buffer, numRuns, 2, new int[] {4, 4});
    File bkmFile = new File(file.getParentFile(), column + ".book1");
    PinotDataBuffer buffer1 = PinotDataBuffer.fromFile(bkmFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "book1_read");
    FixedByteSingleValueMultiColReader bookmarkFileReader = new FixedByteSingleValueMultiColReader(buffer1, numRuns/128, 1, new int[] {4});
    RLEForwardIndexReader rle1_reader = new RLEForwardIndexReader(rawFileReader, bookmarkFileReader, numDocs/numRuns*128, numDocs);

    outputFileName3 = column + ".rle1_read";
    outputFile3 = new PrintWriter(outputFileName3, "UTF-8");
    for (int i = 0; i < numDocs; i++) {
      int v = rle1_reader.getInt(i);
      outputFile3.println(v);
    }
    outputFile3.close();

    outputFileName3 = column + ".rle1_scan";
    outputFile3 = new PrintWriter(outputFileName3, "UTF-8");
    RLEValueReaderContext context = rle1_reader.createContext();
    for (int i = 0; i < numDocs; i++) {
      int v = rle1_reader.getInt(i, context);
      int v1 = rle1_reader.getInt(i);
      if (v != v1) {
        System.out.println("Difference between " + v + " " + v1 + "for " + i + " th row" );
      }

      outputFile3.println(v);
    }
    outputFile3.close();

    File rleFile2 = new File(file.getParentFile(), column + ".rle2");
    PinotDataBuffer buffer2 = PinotDataBuffer.fromFile(rleFile2, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "rle2_read");
    FixedBitSingleValueMultiColReader rawFileReader2 = new FixedBitSingleValueMultiColReader(buffer2, numRuns+1, 2,
        new int[] {columnSizeInBits, RLEBitForwardIndexCreator.getNumOfBits(numDocs)});
    File bkmFile2 = new File(file.getParentFile(), column + ".book2");
    PinotDataBuffer buffer3 = PinotDataBuffer.fromFile(bkmFile2, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "book2_read");
    FixedBitSingleValueMultiColReader bookmarkFileReader2 = new FixedBitSingleValueMultiColReader(buffer3, numRuns/128,
        1, new int[] {RLEBitForwardIndexCreator.getNumOfBits(numRuns+1)});
    RLEBitForwardIndexReader rle2_reader = new RLEBitForwardIndexReader(rawFileReader2, bookmarkFileReader2, numDocs/numRuns*128, numDocs);

    outputFileName3 = column + ".rle2_read";
    outputFile3 = new PrintWriter(outputFileName3, "UTF-8");
    for (int i = 0; i < numDocs; i++) {
      int v = rle2_reader.getInt(i);
      outputFile3.println(v);
    }
    outputFile3.close();

    outputFileName3 = column + ".rle2_scan";
    outputFile3 = new PrintWriter(outputFileName3, "UTF-8");
    context = rle2_reader.createContext();
    for (int i = 0; i < numDocs; i++) {
      int v = rle2_reader.getInt(i, context);
      int v1 = rle2_reader.getInt(i);
      if (v != v1) {
        System.out.println("Difference between " + v + " " + v1 + "for " + i + " th row" );
      }
      outputFile3.println(v);
    }
    outputFile3.close();

    final long randomSeed = 123456789L;
    final int MAX_DOC = 100000;
    int[] docIDArray;
    docIDArray = new int[MAX_DOC];

    Random random = new Random(randomSeed);
    for (int i = 0; i < MAX_DOC; i++){
      docIDArray[i] = random.nextInt(MAX_DOC);
     }
    Arrays.sort(docIDArray);

    context = rle1_reader.createContext();
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (int run = 0; run < MAX_RUNS; run++) {
      start = System.currentTimeMillis();
      for (int i = 0; i < MAX_DOC; i++) {
        int value = rle1_reader.getInt(i, context);
      }
      end = System.currentTimeMillis();
      stats.addValue(end - start);
    }
    System.out.println(" rle random read stats for column: " + column);
    System.out.println(
        stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));

    stats = new DescriptiveStatistics();
    for (int run = 0; run < MAX_RUNS; run++) {
      start = System.currentTimeMillis();
      for (int i = 0; i < MAX_DOC; i++) {
        int value = reader.getInt(i);
      }
      end = System.currentTimeMillis();
      stats.addValue(end - start);
    }
    System.out.println(" fwd index random read stats for column: " + column);
    System.out.println(
        stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));

    context = rle1_reader.createContext();
    stats = new DescriptiveStatistics();
    for (int run = 0; run < MAX_RUNS; run++) {
      start = System.currentTimeMillis();
      for (int i = 0; i < numDocs; i++) {
        int value = rle1_reader.getInt(i, context);
      }
      end = System.currentTimeMillis();
      stats.addValue(end - start);
    }
    System.out.println(" rle sequential read stats for " + column);
    System.out.println(
        stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    rle1_reader.close();

    stats = new DescriptiveStatistics();
    for (int run = 0; run < MAX_RUNS; run++) {
      start = System.currentTimeMillis();
      for (int i = 0; i < numDocs; i++) {
        int value = reader.getInt(i);
      }
      end = System.currentTimeMillis();
      stats.addValue(end - start);
    }
    System.out.println(" v1 sequential read stats for " + file.getName());
    System.out.println(
        stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    reader.close();
    heapBuffer.close();
  }

  public static void singleValuedReadBenchMarkV2(File file, int numDocs, int numBits)
      throws Exception {
  }

  public static void multiValuedReadBenchMarkV1(File file, int numDocs, int totalNumValues,
      int maxEntriesPerDoc, int columnSizeInBits) throws Exception {
  }

  public static void multiValuedReadBenchMarkV2(File file, int numDocs, int totalNumValues,
      int maxEntriesPerDoc, int columnSizeInBits) throws Exception {
  }

  private static void benchmarkForwardIndex(String indexDir) throws Exception {
    benchmarkForwardIndex(indexDir, null);
  }

  private static void benchmarkForwardIndex(String indexDir, List<String> includeColumns)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(indexDir));
    String segmentVersion = segmentMetadata.getVersion();
    Set<String> columns = segmentMetadata.getAllColumns();
    for (String column : columns) {
      if (includeColumns != null && !includeColumns.isEmpty()) {
        if (!includeColumns.contains(column)) {
          continue;
        }
      }

      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

      if (columnMetadata.isSingleValue() && !columnMetadata.isSorted()) {
        String fwdIndexFileName = segmentMetadata.getForwardIndexFileName(column, segmentVersion);
        File fwdIndexFile = new File(indexDir, fwdIndexFileName);
        singleValuedReadBenchMark(segmentVersion, fwdIndexFile, segmentMetadata.getTotalDocs(),
            columnMetadata.getBitsPerElement(), column);
      }
    }
  }

  private static void singleValuedReadBenchMark(String segmentVersion, File fwdIndexFile,
      int totalDocs, int bitsPerElement, String column) throws Exception {
    if (SegmentVersion.v1.name().equals(segmentVersion)) {
      singleValuedReadBenchMarkV1(fwdIndexFile, totalDocs, bitsPerElement, column);
    } else if (SegmentVersion.v2.name().equals(segmentVersion)) {
      singleValuedReadBenchMarkV2(fwdIndexFile, totalDocs, bitsPerElement);
    }
  }

  /**
   * USAGE ForwardIndexReaderBenchmark <indexDir> <comma delimited column_names(optional)>
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String indexDir = args[0];
    if (args.length == 1) {
      benchmarkForwardIndex(indexDir);
    }
    if (args.length == 2) {
      benchmarkForwardIndex(indexDir, Lists.newArrayList(args[1].trim().split(",")));
    }
  }

}
