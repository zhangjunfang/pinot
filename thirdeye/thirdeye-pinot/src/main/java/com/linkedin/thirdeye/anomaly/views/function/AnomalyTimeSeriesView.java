package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;
import org.joda.time.DateTime;

public interface AnomalyTimeSeriesView {
  /** Initializes this function with its configuration, call before getTimeSeriesView */
  void init(AnomalyFunctionDTO spec);

  AnomalyFunctionDTO getSpec();

  /**
   * Returns the data range intervals for constructing the time series view. This method could returns multiple
   * intervals for anomaly functions that need history data for comparison or training, which could be in different
   * data range intervals than the monitoring data (i.e., current values).
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return the data range intervals for constructing the time series view
   */
  List<Pair<Long, Long>> getDataRangeIntervals(DateTime monitoringWindowStartTime,
      DateTime monitoringWindowEndTime);

  /**
   * Given any metric, this method returns the corresponding current and baseline time series to be presented in the
   * frontend. The given metric is not necessary the data that is used for detecting anomaly.
   *
   * For instance, if a function uses the average values of the past 3 weeks as the baseline for anomaly detection,
   * then this method should construct a baseline that contains the average value of the past 3 weeks of the given
   * metric.
   *
   * Note that the usage of this method should be similar to the method {@link AnamalyFunction.analyze},
   * i.e., it does not take care of setting filters, dimension names, etc. for retrieving the data from the backend
   * database. Specifically, it only processes the given data, i.e., timeSeries, for presentation purpose.
   *
   * The only difference between this method and {@link #analyze} is that their bucket sizes are different.
   * This method's bucket size is given by frontend, which should larger or equal to the minimum time granularity of
   * the data. On the other hand, {@link #analyze}'s buckets size is always the minimum time granularity of the data.
   *
   * @param timeSeries the time series that contains the metric to be processed
   * @param metric the metric name to retrieve the data from the given time series
   * @param timeGranularity the time granularity of the given time series
   * @param viewWindowStartTime the start time bucket of current time series, inclusive
   * @param viewWindowEndTime the end time buckets of current time series, exclusive
   * @return Two set of time series: a current and a baseline values, to be represented in the frontend
   */
  AnomalyTimelinesView getTimeSeriesView(MetricTimeSeries timeSeries,
      TimeGranularity timeGranularity, String metric, DateTime viewWindowStartTime,
      DateTime viewWindowEndTime, List<RawAnomalyResultDTO> knownAnomalies);
}
