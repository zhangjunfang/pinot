package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableSegmentConfigs {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSegmentConfigs.class);

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @PUT
  @Path("/tables/{tableName}/segmentConfigs")
  @ApiOperation(value = "Update segments configuration",
      notes = "Updates segmentsConfig section (validation and retention) of a table")
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success"),
      @ApiResponse(code=404, message="Table not found"),
      @ApiResponse(code=500, message = "Internal server error")})
  public SuccessResponse put(@ApiParam(value = "Table name", required = true)
      @PathParam("tableName") String tableName,
      String requestBody) {

    try {
      AbstractTableConfig config = AbstractTableConfig.init(requestBody);
      pinotHelixResourceManager.updateSegmentsValidationAndRetentionConfigFor(config.getTableName(),
          TableType.valueOf(config.getTableType().toUpperCase()), config.getValidationConfig());
      return new SuccessResponse("Update segmentsConfig for table: " + tableName);
    } catch (final Exception e) {
      LOGGER.error("Caught exception while updating segment config ", e);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_SCHEMA_UPDATE_ERROR, 1L);
      throw new WebApplicationException("Error while updating segments config for table: " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

}
