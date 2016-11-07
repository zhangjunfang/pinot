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
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableIndexingConfigs {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIndexingConfigs.class);

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @PUT
  @Path("/tables/{tableName}/indexingConfigs")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Update table indexing configuration")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Server error updating configuration")
  })
  public SuccessResponse updateIndexingConfig(
      @ApiParam(value = "Table name (without type)", required = true)
      @PathParam("tableName") String tableName,
      String body) {
    AbstractTableConfig config = null;
    try {
      config = AbstractTableConfig.init(body);
    } catch (JSONException | IOException e) {
      LOGGER.info("Error converting requet to table config for table: {}", tableName, e);
      throw new WebApplicationException("Error converting request body to table configuration",
          Response.Status.BAD_REQUEST);
    }

    TableType tableType = TableType.valueOf(config.getTableType().toUpperCase());
    if (tableType == TableType.OFFLINE) {
      if (! pinotHelixResourceManager.hasOfflineTable(tableName)) {
        throw new WebApplicationException("Table " +tableName + " does not exist", Response.Status.BAD_REQUEST);
      }
    } else if (tableType == TableType.REALTIME) {
      if (! pinotHelixResourceManager.hasRealtimeTable(tableName)) {
        throw new WebApplicationException("Table " +tableName + " does not exist", Response.Status.BAD_REQUEST);
      }
    }
    try {
      pinotHelixResourceManager.updateIndexingConfigFor(config.getTableName(), tableType, config.getIndexingConfig());
      return new SuccessResponse("Updating indexing config for table " + tableName);
    } catch (Exception e) {
      LOGGER.error("Error updating configuration for table: {}", tableName, e);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      throw new WebApplicationException("Error updating indexing configuration for table " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
