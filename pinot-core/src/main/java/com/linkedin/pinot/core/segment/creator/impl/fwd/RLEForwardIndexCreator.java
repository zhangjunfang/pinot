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
package com.linkedin.pinot.core.segment.creator.impl.fwd;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class RLEForwardIndexCreator implements SingleValueForwardIndexCreator, Closeable {
  private FixedByteSingleValueMultiColWriter indexWriter, bookmarkWriter;
  private int cardinality;
  private int numRuns;
  private int numDocs;
  private int bookmarkDistance;
  private int curDoc = -1;
  private int curDict = -1;
  private int curRow = 0;
  private static final int BOOKMARK_RUNS = 128;

  public RLEForwardIndexCreator(File indexDir, int cardinality, FieldSpec spec, int numRuns, int numDocs) throws Exception {
    File indexFile = new File(indexDir, spec.getName() + /* V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION) */ ".rle1");
    File bookFile = new File(indexDir, spec.getName() + /* V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION) */ ".book1");
    indexWriter = new FixedByteSingleValueMultiColWriter(indexFile, numRuns+1, 2, new int[] { 4, 4 });
    bookmarkDistance = numDocs / numRuns * BOOKMARK_RUNS;
    bookmarkWriter = new FixedByteSingleValueMultiColWriter(bookFile, (numDocs/bookmarkDistance) +1 , 1, new int[] { 4 });
    this.cardinality = cardinality;
    this.numRuns = numRuns;
    this.numDocs = numDocs;
    curDict = -1;
    curDoc = -1;
    curRow = 0;
  }

  public void add(int dictionaryId, int docId) {
    curDoc = docId;
    if ((docId % bookmarkDistance == 0) && (docId > 0)){
      bookmarkWriter.setInt(docId/bookmarkDistance - 1, 0,
          (curDict == dictionaryId) ? curRow-1 : curRow);
    }
    if (curDict == dictionaryId) {
      return;
    } else {
      indexWriter.setInt(curRow, 0, dictionaryId);
      indexWriter.setInt(curRow, 1, docId);
      curDict = dictionaryId;
      curRow++;
    }
  }

  public void seal() throws IOException {
    bookmarkWriter.setInt(curDoc/bookmarkDistance , 0, curRow);
    indexWriter.setInt(curRow, 0, 0);
    indexWriter.setInt(curRow, 1, numDocs);
    indexWriter.close();
    bookmarkWriter.close();
  }

  @Override
  public void index(int docId, int dictionaryIndex) {
    add(dictionaryIndex, docId);
  }

  @Override
  public void close() throws IOException {
    seal();
  }
}
