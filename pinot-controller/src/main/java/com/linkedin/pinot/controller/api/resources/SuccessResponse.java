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
package com.linkedin.pinot.controller.api.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;


public class SuccessResponse {
  private static final Logger LOGGER = LoggerFactory.getLogger(SuccessResponse.class);
  private String status;

  public SuccessResponse(@JsonProperty("status") String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public SuccessResponse setStatus(String status) {
    this.status = status;
    return this;
  }

}
