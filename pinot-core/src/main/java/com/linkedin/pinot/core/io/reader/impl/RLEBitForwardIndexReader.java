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
package com.linkedin.pinot.core.io.reader.impl;

import java.io.IOException;

import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.utils.ByteBufferBinarySearchUtil;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.RLEValueReaderContext;

public class RLEBitForwardIndexReader
    extends BaseSingleColumnSingleValueReader<RLEValueReaderContext> {
  private final FixedBitSingleValueMultiColReader indexReader;
  private final FixedBitSingleValueMultiColReader bookMarkReader;
  private final int numDocs;
  private final int bookMarkDistance;

  public RLEBitForwardIndexReader(FixedBitSingleValueMultiColReader rawFileReader,
      FixedBitSingleValueMultiColReader bookmarkFileReader, int bookMarkDistance,
      int numDocs) {
    indexReader = rawFileReader;
    bookMarkReader = bookmarkFileReader;
    this.numDocs = numDocs;
    this.bookMarkDistance = bookMarkDistance;
  }

  @Override
  public void close() throws IOException {
    // no need to close here , will be closed by parent container

  }

  @Override
  public char getChar(int row) {
    throw new UnsupportedOperationException("not allowed in sorted reader");
  }

  @Override
  public short getShort(int row) {
    throw new UnsupportedOperationException("not allowed in sorted reader");
  }

  @Override
  public int getInt(int docId, RLEValueReaderContext context) {
    if (docId >= context.docIdStart && docId <= context.docIdEnd) {
      return context.value;
    }

    int row = context.row;
    if ((row != -1) && (indexReader.getNumberOfRows() >= row+2)
        && (docId < indexReader.getInt(context.row + 2, 1))) {
      // Next run
      row++;
    } else {
      row = getRow(docId);
    }
    if (row != Constants.EOF) {
      context.row = row;
      context.value = indexReader.getInt(context.row, 0);
      context.docIdStart = indexReader.getInt(context.row, 1);
      context.docIdEnd = indexReader.getInt(context.row + 1, 1) - 1;
    }
    return context.value;
  }

  @Override
  public int getInt(int docId) {
    int row = getRow(docId);
    if (row == Constants.EOF) {
      return 0;
    }
    return indexReader.getInt(row, 0);
  }

  public int getRow(int docId){
    if (indexReader.getNumberOfRows() == 1) {
      return 0;
    }
    if (indexReader.getInt(1, 1) > docId) {
      return 0;
    }
    if (docId > numDocs) {
      return Constants.EOF;
    }
    int bookMark = docId / bookMarkDistance - 1;
    int docIdStart = (bookMark < 0) ? 0 : bookMarkReader.getInt(bookMark, 0);
    int docIdEnd = bookMarkReader.getInt(bookMark + 1, 0);
    int ret = binarySearch(1, docId, docIdStart, docIdEnd);

    if (ret < 0) {
      ret = (ret + 1) * -1;
    }

    if (ret < indexReader.getNumberOfRows()) {
      return ret;
    }

    return Constants.EOF;
  }


  @Override
  public void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos) {
    int endPos = rowStartPos + rowSize;
    for (int ri = rowStartPos; ri < endPos; ++ri) {
      values[valuesStartPos++] = getInt(rows[ri]);
    }
  }

  public int getLength() {
    return numDocs;
  }

  @Override
  public RLEValueReaderContext createContext() {
    return new RLEValueReaderContext();
  }

  /**
   *
   * @param col
   * @param value
   * @param from
   * @param to
   * @return
   */
  public int binarySearch(int col, int value, int from, int to) {
    int low = from;
    int high = to;
    while (low < high) {
      final int middle = (low + high) / 2;
      final int midValue = indexReader.getInt(middle, col);
      final int midValueEnd = indexReader.getInt(middle+1, col);
      if (value >= midValue) {
        if (value > midValueEnd) {
          low = middle + 1;
        } else if (value == midValueEnd) {
          return middle + 1;
        } else {
          return middle;
        }
      } else {
        high = middle - 1;
      }
    }
    return low;
  }
}
