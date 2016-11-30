package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.TimeRangeUtils;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseAnomalyTimeSeriesView implements com.linkedin.thirdeye.anomaly.views.function.AnomalyTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAnomalyTimeSeriesView.class);

  protected AnomalyFunctionDTO spec;
  protected Properties properties;

  @Override
  public void init(AnomalyFunctionDTO spec) {
    this.spec = spec;
    try {
      this.properties = AnomalyTimeSeriesViewUtils.getPropertiesFromSpec(spec);
    } catch (IOException e) {
      this.properties = new Properties();
      LOG.warn("Failed to read anomaly function spec from {}", spec.getFunctionName());
    }
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  /**
   * Returns the data range intervals for showing current time series without baseline.
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return the data range intervals for only showing current time series
   */
  @Override
  public List<Pair<Long, Long>> getDataRangeIntervals(DateTime monitoringWindowStartTime, DateTime monitoringWindowEndTime) {
    List<Pair<Long, Long>> startEndTimeIntervals = new ArrayList<>();
    startEndTimeIntervals.add(new Pair<>(monitoringWindowStartTime.getMillis(),
        monitoringWindowEndTime.getMillis()));

    return startEndTimeIntervals;
  }

  /**
   * Returns the values of current time series
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param metric the metric name to retrieve the data from the given time series
   * @param timeGranularity the time granularity of the given time series
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @return the current time series to be represented in the frontend
   */
  public AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries, TimeGranularity timeGranularity, String metric,
      DateTime viewWindowStartTime, DateTime viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = new AnomalyTimelinesView();

    int bucketCount =
        TimeRangeUtils.computeBucketCount(viewWindowStartTime, viewWindowEndTime, timeGranularity);

    // Construct AnomalyTimelinesView for current time series without baseline
    DateTime currentTime = viewWindowStartTime;
    for (int i = 0; i < bucketCount; ++i) {
      DateTime nextCurrentTime = TimeRangeUtils.increment(currentTime, timeGranularity);

      TimeBucket timebucket = new TimeBucket(currentTime.getMillis(), nextCurrentTime.getMillis(),
          currentTime.getMillis(), nextCurrentTime.getMillis());
      anomalyTimelinesView.addTimeBuckets(timebucket);

      anomalyTimelinesView.addCurrentValues(timeSeries.get(currentTime.getMillis(), metric).doubleValue());

      currentTime = nextCurrentTime;
    }

    return anomalyTimelinesView;
  }
}
