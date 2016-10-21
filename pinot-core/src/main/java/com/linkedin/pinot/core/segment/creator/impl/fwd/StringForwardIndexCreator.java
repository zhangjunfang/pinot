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
import java.io.FileOutputStream;
import java.io.IOException;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;
import com.linkedin.pinot.core.segment.creator.SingleValueForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class StringForwardIndexCreator implements SingleValueForwardIndexCreator, Closeable {
  private FileOutputStream indexWriter;
  private FixedByteSingleValueMultiColWriter  bookmarkWriter;
  private int cardinality;
  private int numDocs;
  private int curDoc = -1;
  private int curDict = -1;
  private int curLoc = 0;

  public StringForwardIndexCreator(File indexDir, int cardinality, FieldSpec spec, int numRuns, int numDocs) throws Exception {
    File indexFile = new File(indexDir, spec.getName() + /* V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION) */ ".sfwd");
    File bookFile = new File(indexDir, spec.getName() + /* V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION) */ ".sind");
    indexWriter = new FileOutputStream(indexFile);
    bookmarkWriter = new FixedByteSingleValueMultiColWriter(bookFile, numDocs +1 , 1, new int[] { 4 });
    this.cardinality = cardinality;
    this.numDocs = numDocs;
    curDict = -1;
    curDoc = -1;
    curLoc = 0;
  }

  public void add(String value, int docId) {
    curDoc = docId;
    bookmarkWriter.setInt(docId, 0, curLoc);
    try {
      indexWriter.write(value.getBytes(), 0, value.getBytes().length);
    } catch (Exception e)
    {
    }
    curLoc += value.getBytes().length;
    curDoc++;
  }

  public void seal() throws IOException {
    bookmarkWriter.setInt(curDoc, 0, curLoc);
    indexWriter.close();
    bookmarkWriter.close();
  }

  public void index(int docId, String value) {
    add(value, docId);
  }

  @Override
  public void index(int docId, int dictionaryIndex) {
    throw new RuntimeException("String Forward dictionary uses string as value");
  }

  @Override
  public void close() throws IOException {
    seal();
  }
}
