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

package com.linkedin.pinot.core.data.filter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.common.utils.SegmentName;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dictionary based duplicate key row filter.
 */
public class DictionaryGenericRowFilter implements GenericRowFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryGenericRowFilter.class);

  private final String _keyColumn;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final List<Pair<Dictionary, BloomFilter>> _dictionariesAndBloomFilters;
  private final List<SegmentDataManager> _acquiredSegments;
  private long bloomFilterTests = 0;
  private long bloomFilterHits = 0;
  private long dictionaryTests = 0;
  private long dictionaryHits = 0;
  private long bloomFilterNanos = 0;
  private long dictionaryNanos = 0;

  public DictionaryGenericRowFilter(String keyColumn, Dictionary currentSegmentDictionary, LLCSegmentName currentSegmentName, RealtimeTableDataManager realtimeTableDataManager) {
    _keyColumn = keyColumn;
    _realtimeTableDataManager = realtimeTableDataManager;
    _dictionariesAndBloomFilters = new ArrayList<>();
    _acquiredSegments = new ArrayList<>();

    // Keep the dictionary for this segment
    _dictionariesAndBloomFilters.add(new ImmutablePair<Dictionary, BloomFilter>(currentSegmentDictionary, null));

    // Get dictionaries for other segments
    // TODO jfim: There is no guarantee at this point that this covers all segments, since a consuming segment might process its state transition before an already stored segment (eg. when if a server restarts)
    List<SegmentDataManager> allSegments = realtimeTableDataManager.acquireAllSegments();

    for (SegmentDataManager segment : allSegments) {
      String segmentNameStr = segment.getSegmentName();

      // Ignore non LLC segments
      if (!SegmentName.isLowLevelConsumerSegmentName(segmentNameStr)) {
        realtimeTableDataManager.releaseSegment(segment);
        continue;
      }

      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);

      // Keep segments for this table and partition
      if (segmentName.getTableName().equals(currentSegmentName.getTableName()) && segmentName.getPartitionId() == currentSegmentName.getPartitionId()) {
        _acquiredSegments.add(segment);
        DataSource dataSource = segment.getSegment().getDataSource(keyColumn);
        Dictionary dictionary = dataSource.getDictionary();
        BloomFilter bloomFilter = dataSource.getBloomFilter();
        _dictionariesAndBloomFilters.add(new ImmutablePair<Dictionary, BloomFilter>(dictionary, bloomFilter));
      } else {
        realtimeTableDataManager.releaseSegment(segment);
      }
    }
  }

  @Override
  public GenericRow filter(GenericRow genericRow) {
    if (_keyColumn == null || _dictionariesAndBloomFilters == null) {
      return genericRow;
    }

    Object keyValue = genericRow.getValue(_keyColumn);

    if (keyValue == null) {
      return genericRow;
    }

    // Does this key already exist in any of the dictionaries/bloom filters?
    for (Pair<Dictionary, BloomFilter> dictionaryAndBloomFilter : _dictionariesAndBloomFilters) {
      Dictionary dictionary = dictionaryAndBloomFilter.getLeft();
      BloomFilter bloomFilter = dictionaryAndBloomFilter.getRight();

      if (bloomFilter != null) {
        long bloomStartNanos = System.nanoTime();
        byte[] value;
        if (keyValue instanceof Integer) {
          value = Ints.toByteArray((Integer) keyValue);
        } else if (keyValue instanceof Long){
          value = Longs.toByteArray((Long) keyValue);
        } else {
          // TODO jfim Implement other types
          throw new RuntimeException("Unimplemented!");
        }

        // Skip the dictionary lookup if this value cannot be present
        bloomFilterTests++;
        boolean isPresentInBloomFilter = bloomFilter.isPresent(value);
        long bloomEndNanos = System.nanoTime();
        bloomFilterNanos += (bloomEndNanos - bloomStartNanos);
        if (!isPresentInBloomFilter) {
          continue;
        } else {
          bloomFilterHits++;
        }
      }

      dictionaryTests++;
      long dictionaryStart = System.nanoTime();
      int indexOf = dictionary.indexOf(keyValue);
      long dictionaryEnd = System.nanoTime();
      dictionaryNanos += (dictionaryEnd - dictionaryStart);
      if (0 <= indexOf) {
        dictionaryHits++;
        System.out.println("Discarding keyValue = " + keyValue);
        // Yes, discard row
        return null;
      }
    }
    
    return genericRow;
  }

  @Override
  public synchronized void close() {
    if (!_acquiredSegments.isEmpty()) {
      for (SegmentDataManager acquiredSegment : _acquiredSegments) {
        _realtimeTableDataManager.releaseSegment(acquiredSegment);
      }

      _acquiredSegments.clear();

      LOGGER.info(
          "Closing filter, bloom tests {} / {} ({} ms total) dictionary {} / {} ({} ms total), lookback of {} segments",
          bloomFilterHits, bloomFilterTests, bloomFilterNanos / 1000000.0, dictionaryHits, dictionaryTests,
          dictionaryNanos / 1000000.0, _dictionariesAndBloomFilters.size());
    }

/*    System.out.println("Bloom filter hits/tests: " + bloomFilterHits + "/" + bloomFilterTests);
    System.out.println("Dictionary hits/tests  : " + dictionaryHits + "/" + dictionaryTests); */
  }
}
