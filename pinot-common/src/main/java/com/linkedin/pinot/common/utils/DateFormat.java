package com.linkedin.pinot.common.utils;

import org.json.JSONObject;


public class DateFormat extends JSONObject {
  private static String _simpleDate;

  public DateFormat(String simpleDate) {
    _simpleDate = simpleDate;
  }

  public String getSimpleDate() {
    return _simpleDate;
  }

  public void setSimpleDate(String simpleDate) {
    _simpleDate = simpleDate;
  }
}
