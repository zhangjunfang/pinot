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
package com.linkedin.pinot.core.operator.aggregation;

import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;


/**
 * This class caches miscellaneous data to perform efficient aggregation.
 */
public class AggregationFunctionContext {
  private final AggregationInfo _aggregationInfo;
  private final AggregationFunction _aggregationFunction;
  private final String[] _aggrColumns;

  /**
   * Constructor for the class.
   *
   * @param aggFuncName
   * @param aggrColumns
   */
  public AggregationFunctionContext(AggregationInfo aggregationInfo, Map<String,Object> functionArguments) {
    this._aggregationInfo = aggregationInfo;
    _aggrColumns = aggregationInfo.getAggregationParams().get("column").trim().split(",");
    String aggFuncName = aggregationInfo.getAggregationType();
    _aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggFuncName, functionArguments);
  }

  /**
   * Returns the aggregation function object.
   * @return
   */
  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * Returns an array of aggregation column names.
   * @return
   */
  public String[] getAggregationColumns() {
    return _aggrColumns;
  }
  
  public AggregationInfo getAggregationInfo() {
    return _aggregationInfo;
  }
}
