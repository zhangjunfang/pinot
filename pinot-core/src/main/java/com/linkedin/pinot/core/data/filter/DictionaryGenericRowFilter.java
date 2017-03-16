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
import com.linkedin.pinot.core.data.manager.offline.OfflineSegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import com.linkedin.pinot.core.realtime.RealtimeSegment;
import com.linkedin.pinot.core.realtime.impl.kafka.Blah;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dictionary based duplicate key row filter.
 */
public class DictionaryGenericRowFilter implements GenericRowFilter {
  private static class TimeRange {
    long start;
    long end;

    public TimeRange(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public boolean isTimestampWithinRange(long timestamp) {
      return start <= timestamp && timestamp <= end;
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryGenericRowFilter.class);

  private final String _keyColumn;
  private final String _timeColumnName;
  private final RealtimeTableDataManager _realtimeTableDataManager;
  private final List<Triple<Dictionary, BloomFilter, TimeRange>> _dictionariesAndBloomFilters;
  private final List<Triple<Dictionary, BloomFilter, TimeRange>> _acrossDictionariesAndBloomFilters;
  private final List<SegmentDataManager> _acquiredSegments;

  private final Dictionary _currentSegmentDictionary;
  private long currentDictionaryNanos = 0;
  private long currentDictionaryTests = 0;
  private long currentDictionaryHits = 0;

  private long timeRangeTests = 0;
  private long timeRangeHits = 0;
  private long acrossTimeRangeTests = 0;
  private long acrossTimeRangeHits = 0;
  private long bloomFilterTests = 0;
  private long bloomFilterHits = 0;
  private long dictionaryTests = 0;
  private long dictionaryHits = 0;
  private long bloomFilterNanos = 0;
  private long dictionaryNanos = 0;
  private long acrossBloomFilterTests = 0;
  private long acrossBloomFilterHits = 0;
  private long acrossDictionaryTests = 0;
  private long acrossDictionaryHits = 0;
  private long acrossBloomFilterNanos = 0;
  private long acrossDictionaryNanos = 0;

  public DictionaryGenericRowFilter(String keyColumn, Dictionary currentSegmentDictionary, LLCSegmentName currentSegmentName, RealtimeTableDataManager realtimeTableDataManager, String timeColumnName) {
    _keyColumn = keyColumn;
    _realtimeTableDataManager = realtimeTableDataManager;
    _dictionariesAndBloomFilters = new ArrayList<>();
    _acrossDictionariesAndBloomFilters = new ArrayList<>();
    _acquiredSegments = new ArrayList<>();
    _timeColumnName = timeColumnName;

    // Keep the dictionary for this segment
    _currentSegmentDictionary = currentSegmentDictionary;

    // Get dictionaries for other segments
    // TODO jfim: There is no guarantee at this point that this covers all segments, since a consuming segment might process its state transition before an already stored segment (eg. when if a server restarts)
    List<SegmentDataManager> allSegments = realtimeTableDataManager.acquireAllSegments();

    for (SegmentDataManager segment : allSegments) {
      String segmentNameStr = segment.getSegmentName();

      // Ignore current realtime segments
      if (segment.getSegment() instanceof RealtimeSegment) {
        realtimeTableDataManager.releaseSegment(segment);
        continue;
      }

      // Ignore non LLC segments
      if (!SegmentName.isLowLevelConsumerSegmentName(segmentNameStr)) {
        realtimeTableDataManager.releaseSegment(segment);
        continue;
      }

      LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);

      // Keep segments for this table and partition
      if (segmentName.getTableName().equals(currentSegmentName.getTableName())) {
        _acquiredSegments.add(segment);
        boolean isSamePartition = segmentName.getPartitionId() == currentSegmentName.getPartitionId();
        DataSource dataSource = segment.getSegment().getDataSource(keyColumn);
        Dictionary keyColumnDictionary = dataSource.getDictionary();
        BloomFilter keyColumnBloomFilter = dataSource.getBloomFilter();

        Dictionary timeColumnDictionary = segment.getSegment().getDataSource(timeColumnName).getDictionary();
        long startTime = timeColumnDictionary.getLongValue(0);
        long endTime = timeColumnDictionary.getLongValue(timeColumnDictionary.length() - 1);
        TimeRange timeRange = new TimeRange(startTime, endTime);

        if (isSamePartition) {
          _dictionariesAndBloomFilters.add(new ImmutableTriple<>(keyColumnDictionary, keyColumnBloomFilter, timeRange));
        } else {
          _acrossDictionariesAndBloomFilters.add(new ImmutableTriple<>(keyColumnDictionary, keyColumnBloomFilter, timeRange));
        }
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

    byte[] value;
    if (keyValue instanceof Integer) {
      value = Ints.toByteArray((Integer) keyValue);
    } else if (keyValue instanceof Long){
      value = Longs.toByteArray((Long) keyValue);
    } else {
      // TODO jfim Implement other types
      throw new RuntimeException("Unimplemented!");
    }

    long currentDictionaryNanoStart = System.nanoTime();
    boolean isInCurrentDictionary = 0 <= _currentSegmentDictionary.indexOf(keyValue);
    long currentDictionaryNanoEnd = System.nanoTime();
    currentDictionaryNanos += (currentDictionaryNanoEnd - currentDictionaryNanoStart);

    currentDictionaryTests++;
    if (isInCurrentDictionary) {
      currentDictionaryHits++;
      LOGGER.info("Discarding current keyValue = " + keyValue);
      return null;
    }

    // Does this key already exist in any of the dictionaries/bloom filters?
    for (Triple<Dictionary, BloomFilter, TimeRange> dictionaryAndBloomFilter : _dictionariesAndBloomFilters) {
      Dictionary dictionary = dictionaryAndBloomFilter.getLeft();
      BloomFilter bloomFilter = dictionaryAndBloomFilter.getMiddle();
      TimeRange timeRange = dictionaryAndBloomFilter.getRight();

      if (Blah.ENABLE_TIME_RANGE_FILTERING) {
        boolean inTimeRange = timeRange.isTimestampWithinRange(((Number) genericRow.getValue(_timeColumnName)).longValue());
        timeRangeTests++;
        if (!inTimeRange) {
          continue;
        } else {
          timeRangeHits++;
        }
      }

      if (bloomFilter != null) {
        long bloomStartNanos = System.nanoTime();

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
        LOGGER.info("Discarding keyValue = " + keyValue);
        // Yes, discard row
        return null;
      }
    }

    for (Triple<Dictionary, BloomFilter, TimeRange> dictionaryAndBloomFilter : _acrossDictionariesAndBloomFilters) {
      Dictionary dictionary = dictionaryAndBloomFilter.getLeft();
      BloomFilter bloomFilter = dictionaryAndBloomFilter.getMiddle();
      TimeRange timeRange = dictionaryAndBloomFilter.getRight();

      boolean inTimeRange = timeRange.isTimestampWithinRange(((Number) genericRow.getValue(_timeColumnName)).longValue());
      acrossTimeRangeTests++;
      if (!inTimeRange) {
        continue;
      } else {
        acrossTimeRangeHits++;
      }

      if (bloomFilter != null) {
        long bloomStartNanos = System.nanoTime();

        // Skip the dictionary lookup if this value cannot be present
        acrossBloomFilterTests++;
        boolean isPresentInBloomFilter = bloomFilter.isPresent(value);
        long bloomEndNanos = System.nanoTime();
        acrossBloomFilterNanos += (bloomEndNanos - bloomStartNanos);
        if (!isPresentInBloomFilter) {
          continue;
        } else {
          acrossBloomFilterHits++;
        }
      }

      acrossDictionaryTests++;
      long dictionaryStart = System.nanoTime();
      int indexOf = dictionary.indexOf(keyValue);
      long dictionaryEnd = System.nanoTime();
      acrossDictionaryNanos += (dictionaryEnd - dictionaryStart);
      if (0 <= indexOf) {
        acrossDictionaryHits++;
        LOGGER.info("Discarding across keyValue = " + keyValue);
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

      LOGGER.info("Closing filter, same segment stats: {} / {} ({} ms total)", currentDictionaryHits, currentDictionaryTests, currentDictionaryNanos / 1000000.0);
      LOGGER.info(
          "Same partition: time tests {} / {}, bloom tests {} / {} ({} ms total) dictionary {} / {} ({} ms total), lookback of {} segments",
          timeRangeHits, timeRangeTests,
          bloomFilterHits, bloomFilterTests, bloomFilterNanos / 1000000.0, dictionaryHits, dictionaryTests,
          dictionaryNanos / 1000000.0, _dictionariesAndBloomFilters.size());
      LOGGER.info(
          "Across partitions: time tests {} / {}, bloom tests {} / {} ({} ms total) dictionary {} / {} ({} ms total), lookback of {} segments",
          acrossTimeRangeHits, acrossTimeRangeTests,
          acrossBloomFilterHits, acrossBloomFilterTests, acrossBloomFilterNanos / 1000000.0, acrossDictionaryHits, acrossDictionaryTests,
          acrossDictionaryNanos / 1000000.0, _acrossDictionariesAndBloomFilters.size());
    }

/*    LOGGER.info("Bloom filter hits/tests: " + bloomFilterHits + "/" + bloomFilterTests);
    LOGGER.info("Dictionary hits/tests  : " + dictionaryHits + "/" + dictionaryTests); */
  }
}
