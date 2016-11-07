package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableSchema.class);

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @GET
  @Path("/tables/{tableName}/schema")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get table schema", notes = "Read table schema")
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found")})
  public Schema getTableSchema(
      @ApiParam(value = "Table name (without type)", required = true)
      @PathParam("tableName") String tableName
  ) {
    if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      return readSchemaForTableType(tableName, CommonConstants.Helix.TableType.REALTIME);
    } else {
      return readSchemaForTableType(tableName, CommonConstants.Helix.TableType.OFFLINE);
    }
  }

  private Schema readSchemaForTableType(String tableName, CommonConstants.Helix.TableType tableType) {
    try {
      AbstractTableConfig config = pinotHelixResourceManager.getTableConfig(tableName, tableType);
      if (config == null) {
        throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.NOT_FOUND);
      }
      Schema schema = pinotHelixResourceManager.getSchema(config.getValidationConfig().getSchemaName());
      if (schema == null) {
        throw new WebApplicationException("Table " + tableName + " doest not exist", Response.Status.NOT_FOUND);
      }
      return schema;

    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching schema for a realtime table : {} ", tableName, e);

      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_SCHEMA_GET_ERROR, 1L);
      throw new WebApplicationException("Internal server error while reading schema for table " + tableName,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
