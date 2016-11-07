package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceManagerResponse;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.codehaus.jackson.annotate.JsonProperty;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableRestletResource  {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableRestletResource.class);

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;

  @POST
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  // @Consumes may break clients. We don't want to do that yet
  @ApiOperation(value = "Create a table", notes = "Creates a new table from json configuration")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 400, message = "Bad request. Invalid table configuration"),
      @ApiResponse(code=409, message="Conflict; table already exists"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public SuccessResponse createTable(String requestBody) {
    AbstractTableConfig config;
    try {
      config = AbstractTableConfig.init(requestBody);
    } catch (Exception e) {
      LOGGER.info("Error deserializing request json for table creation", e);
      throw new WebApplicationException("Bad json configuration for table creation request",
          Response.Status.BAD_REQUEST);
    }
    try {
      TableType type = TableType.valueOf(config.getTableType());
      if (type == TableType.OFFLINE && pinotHelixResourceManager.hasOfflineTable(config.getTableName())) {
        throw new WebApplicationException("Table " + config.getTableName() + " already exists");
      } else if (type == TableType.REALTIME && pinotHelixResourceManager.hasRealtimeTable(config.getTableName())) {
        throw new WebApplicationException("Tabe " + config.getTableName() + " already exists");
      }
      pinotHelixResourceManager.addTable(config);
      return new SuccessResponse("Table successfully created");
    } catch (Exception e) {
      LOGGER.error("Caught exception while adding table", e);
        metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_ADD_ERROR, 1L);
      throw new WebApplicationException("Failed to create table", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }

  public static class Tables {
    List<String> tables;
  }
  /**
   * URI Mappings:
   * - "/tables", "/tables/": List all the tables
   * - "/tables/{tableName}", "/tables/{tableName}/": List config for specified table.
   *
   * - "/tables/{tableName}?state={state}"
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *
   * - "/tables/{tableName}?type={type}"
   *   List all tables of specified type, type can be one of {offline|realtime}.
   *
   *   Set the state for the specified {tableName} to the specified {state} (enable|disable|drop).
   *   * - "/tables/{tableName}?state={state}&amp;type={type}"
   *
   *   Set the state for the specified {tableName} of specified type to the specified {state} (enable|disable|drop).
   *   Type here is type of the table, one of 'offline|realtime'.
   * {@inheritDoc}
   * @see org.restlet.resource.ServerResource#get()
   */
  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  // @Consumes may break clients. We don't want to do that yet
  @ApiOperation(value = "List all tables", notes = "List all the tables")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public String listTables() {

  }

  @Override
  @Get
  public Representation get() {
    final String tableName = (String) getRequest().getAttributes().get(TABLE_NAME);
    final String state = getReference().getQueryAsForm().getValues(STATE);
    final String tableType = getReference().getQueryAsForm().getValues(TABLE_TYPE);

    if (tableType != null && !isValidTableType(tableType)) {
      LOGGER.error(INVALID_TABLE_TYPE_ERROR);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(INVALID_TABLE_TYPE_ERROR);
    }

    if (tableName == null) {
      try {
        return getAllTables();
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching table ", e);
        ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
        setStatus(Status.SERVER_ERROR_INTERNAL);
        return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
      }
    }
    try {
      if (state == null) {
        return getTable(tableName, tableType);
      } else if (isValidState(state)) {
        return setTablestate(tableName, tableType, state);
      } else {
        LOGGER.error(INVALID_STATE_ERROR);
        setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
        return new StringRepresentation(INVALID_STATE_ERROR);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while fetching table ", e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_GET_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  static class TableConfig {
    @JsonProperty("OFFLINE")
    AbstractTableConfig offline;
    @JsonProperty("REALTIME")
    AbstractTableConfig realtime;
  }

  @GET
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Get table configuration", notes = "Gives full table configuration")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Server error reading table configuration")
  })
  public TableConfig getTableConfiguration(
      @ApiParam(value = "Table name (no type)", required = true)
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type", required = false, defaultValue = "",
          example = "OFFLINE", allowableValues = "[OFFLINE,ONLINE")
      @QueryParam("type") @DefaultValue("") String type) {
    TableConfig tableConfig = new TableConfig();
    try {
      if (type == null || type.isEmpty() || TableType.OFFLINE.name().equalsIgnoreCase(type)) {
        tableConfig.offline = pinotHelixResourceManager.getTableConfig(tableName, TableType.OFFLINE);
      }
      if (type == null || type.isEmpty() || TableType.REALTIME.name().equalsIgnoreCase(type)) {
        tableConfig.realtime = pinotHelixResourceManager.getTableConfig(tableName, TableType.REALTIME);
      }
    } catch (Exception e) {
      LOGGER.error("Error reading configuration for table: {}", tableName, e);
      throw new WebApplicationException("Error reading table configuration", Response.Status.INTERNAL_SERVER_ERROR);
    }
    if (tableConfig.offline == null && tableConfig.realtime == null) {
      throw new WebApplicationException("Table configuration not found", Response.Status.NOT_FOUND);
    }
    return tableConfig;
  }


  @HttpVerb("get")
  @Summary("Views all tables' configuration")
  @Tags({ "table" })
  @Paths({ "/tables", "/tables/" })
  private Representation getAllTables() throws JSONException {
    JSONObject object = new JSONObject();
    JSONArray tableArray = new JSONArray();
    Set<String> tableNames = new TreeSet<String>();
    tableNames.addAll(pinotHelixResourceManager.getAllUniquePinotRawTableNames());
    for (String pinotTableName : tableNames) {
      tableArray.put(pinotTableName);
    }
    object.put("tables", tableArray);
    return new StringRepresentation(object.toString(2));
  }

  @HttpVerb("get")
  @Summary("Enable, disable or drop a table")
  @Tags({ "table" })
  @Paths({ "/tables/{tableName}", "/table/{tableName}/" })
  private StringRepresentation setTablestate(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to toggle its state",
          required = true) String tableName,
      @Parameter(name = "type", in = "query", description = "Type of table, Offline or Realtime", required = false) String type,
      @Parameter(name = "state", in = "query", description = "The desired table state, either enable or disable",
          required = true) String state) throws JSONException {

    JSONArray ret = new JSONArray();
    boolean tableExists = false;

    if ((type == null || TableType.OFFLINE.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasOfflineTable(tableName)) {
      String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject offline = new JSONObject();
      tableExists = true;

      offline.put(TABLE_NAME, offlineTableName);
      offline.put(STATE, toggleTableState(offlineTableName, state).toJSON().toString());
      ret.put(offline);
    }

    if ((type == null || TableType.REALTIME.name().equalsIgnoreCase(type))
        && _pinotHelixResourceManager.hasRealtimeTable(tableName)) {
      String realTimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
      JSONObject realTime = new JSONObject();
      tableExists = true;

      realTime.put(TABLE_NAME, realTimeTableName);
      realTime.put(STATE, toggleTableState(realTimeTableName, state).toJSON().toString());
      ret.put(realTime);
    }
    if (tableExists) {
      return new StringRepresentation(ret.toString());
    } else {
      setStatus(Status.CLIENT_ERROR_NOT_FOUND);
      return new StringRepresentation("Error: Table " + tableName + " not found.");
    }
  }

  /**
   * Set the state of the specified table to the specified value.
   *
   * @param tableName: Name of table for which to set the state.
   * @param state: One of [enable|disable|drop].
   * @return
   */
  private PinotResourceManagerResponse toggleTableState(String tableName, String state) {
    if (StateType.ENABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, true);
    } else if (StateType.DISABLE.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.toggleTableState(tableName, false);
    } else if (StateType.DROP.name().equalsIgnoreCase(state)) {
      return _pinotHelixResourceManager.dropTable(tableName);
    } else {
      LOGGER.error(INVALID_STATE_ERROR);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new PinotResourceManagerResponse(INVALID_STATE_ERROR, false);
    }
  }

  @DELETE
  @Path("/tables/{tableName}")
  @ApiOperation(value = "Delete a table", notes = "Deletes a table")
  @ApiResponses(value = {@ApiResponse(code=200, message="Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message =  "Internal server error")})
  public SuccessResponse deleteTable(
      @ApiParam(value = "Table name without type", required = true)
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Table type", required = false, example = "offline",
          allowableValues = "[offline, realtime]")
      @DefaultValue("")
      @QueryParam(Constants.TABLE_TYPE) String tableType) {

    if (tableType.isEmpty() || tableType.equalsIgnoreCase(TableType.OFFLINE.name())) {
      pinotHelixResourceManager.deleteOfflineTable(tableName);
    }
    if (tableType.isEmpty() || tableType.equalsIgnoreCase(TableType.REALTIME.name())) {
      pinotHelixResourceManager.deleteRealtimeTable(tableName);
    }
    return new SuccessResponse("Deleted table " + tableName);
  }
}
