package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.TimeRangeUtils;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeekOverWeekAnomalyTimeSeriesView extends BaseAnomalyTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(WeekOverWeekAnomalyTimeSeriesView.class);

  public static final String BASELINE = "baseline";

  DateTime getBaselineDateTime(DateTime currentDateTime, String baselineProp) {
    int offsetDays = 7;
    if ("w/w".equals(baselineProp)) {
      offsetDays = 7;
    } else if ("w/2w".equals(baselineProp)) {
      offsetDays = 14;
    } else if ("w/3w".equals(baselineProp)) {
      offsetDays = 21;
    } else {
      throw new IllegalArgumentException("Unknown baseline property: " + baselineProp);
    }

    return currentDateTime.minusDays(offsetDays);
  }

  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(DateTime viewWindowStartTime, DateTime
      viewWindowEndTime) {
    // Add current start and end time
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(viewWindowStartTime.getMillis(), viewWindowEndTime.getMillis()));

    // Add baseline start and end time
    String baselineProp = properties.getProperty(BASELINE, null);
    if (baselineProp == null) {
      LOG.warn("Unable to get baseline property; Using default value: w/w");
      baselineProp = "w/w";
    }
    DateTime baselineStartTime = getBaselineDateTime(viewWindowStartTime, baselineProp);
    DateTime baselineEndTime = getBaselineDateTime(viewWindowEndTime, baselineProp);
    startEndTimeIntervals.add(new Pair<>(baselineStartTime.getMillis(), baselineEndTime.getMillis()));

    return startEndTimeIntervals;
  }

  @Override public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries,
      TimeGranularity timeGranularity, String metric, DateTime viewWindowStartTime,
      DateTime viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    // Compute baseline for comparison
    String baselineProp = properties.getProperty(BASELINE, null);
    if (baselineProp == null) {
      LOG.warn("Unable to get baseline property; Using default value: w/w");
      baselineProp = "w/w";
    }
    DateTime baselineTime = getBaselineDateTime(viewWindowStartTime, baselineProp);

    int bucketCount =
        TimeRangeUtils.computeBucketCount(viewWindowStartTime, viewWindowEndTime, timeGranularity);

    // Construct AnomalyTimelinesView
    DateTime currentTime = viewWindowStartTime;
    for (int i = 0; i < bucketCount; ++i) {
      DateTime nextCurrentTime = TimeRangeUtils.increment(currentTime, timeGranularity);
      DateTime nextBaselineTime = getBaselineDateTime(nextCurrentTime, baselineProp);

      double currentValue = timeSeries.get(currentTime.getMillis(), metric).doubleValue();
      double baselineValue = timeSeries.get(baselineTime.getMillis(), metric).doubleValue();
      anomalyTimelinesView.addCurrentValues(currentValue);
      anomalyTimelinesView.addBaselineValues(baselineValue);

      TimeBucket timebucket =
          new TimeBucket(currentTime.getMillis(), nextCurrentTime.getMillis(),
              baselineTime.getMillis(), nextBaselineTime.getMillis());
      anomalyTimelinesView.addTimeBuckets(timebucket);

      currentTime = nextCurrentTime;
      baselineTime = nextBaselineTime;
    }

    return anomalyTimelinesView;
  }

  public static void main(String[] argc) {
    DateTime time = new DateTime(2016, 11, 6, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    DateTime endTime = new DateTime(2016, 11, 7, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    int i = 0;
    while (endTime.compareTo(time) > 0) {
      System.out.println(++i + " " + time + " " + time.minusDays(7));
      time = time.plusHours(1);
    }
    System.out.println();

    time = new DateTime(2016, 11, 13, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    endTime = new DateTime(2016, 11, 14, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    i = 0;
    while (endTime.compareTo(time) > 0) {
      System.out.println(++i + " " + time + " " + time.minusDays(7));
      time = time.plusHours(1);
    }
    System.out.println();

    time = new DateTime(2016, 3, 13, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    endTime = new DateTime(2016, 3, 14, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    i = 0;
    while (endTime.compareTo(time) > 0) {
      System.out.println(++i + " " + time + " " + time.minusDays(7));
      time = time.plusHours(1);
    }
    System.out.println();

    time = new DateTime(2016, 3, 20, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    endTime = new DateTime(2016, 3, 21, 0, 0, DateTimeZone.forID("America/Los_Angeles"));
    i = 0;
    while (endTime.compareTo(time) > 0) {
      System.out.println(++i + " " + time + " " + time.minusDays(7));
      time = time.plusHours(1);
    }
  }
}
