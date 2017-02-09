package com.linkedin.thirdeye.anomalydetection.alertFunctionAutotune;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/8/17.
 */
public abstract class BaseAlertFunctionAutotune {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  public void init(AnomalyFunctionDTO functionSpec){

  }
}