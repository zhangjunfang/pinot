package com.linkedin.pinot.controller.api.resources;

import com.alibaba.fastjson.JSONArray;
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
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableInstances {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);

  @Inject
  ControllerConf controllerConf;
  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;
  @Inject
  ControllerMetrics metrics;


  @GET
  @Path("/tables/{tableName}/instances")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "List table instances", notes = "List instances of the give table")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"),
      @ApiResponse(code = 404, message = "Table not found"),
      @ApiResponse(code = 500, message = "Internal server error")})
  public String getTableInstances(
      @ApiParam(value = "Table name without type", required = true)
      @PathParam("tableName") String tableName,
      @ApiParam(value = "Instance type", required = false, example = "broker",
          allowableValues = "[broker, server]")
      @DefaultValue("")
      @QueryParam("type") String instanceType) {

    try {
      JSONObject ret = new JSONObject();
      ret.put("tableName", tableName);
      JSONArray brokers = new JSONArray();
      JSONArray servers = new JSONArray();

      if (instanceType.isEmpty()|| instanceType.toLowerCase().equals("broker")) {

        if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
          brokers.add(getInstances(pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.OFFLINE),
              TableType.OFFLINE));
        }

        if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          brokers.add(
              getInstances(pinotHelixResourceManager.getBrokerInstancesForTable(tableName, TableType.REALTIME),
                  TableType.REALTIME)
          );
        }
      }

      if (instanceType.isEmpty()|| instanceType.toLowerCase().equals("server")) {
        if (pinotHelixResourceManager.hasOfflineTable(tableName)) {
          JSONObject offlineServers  =
              getInstances(pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.OFFLINE),
                  TableType.OFFLINE);
          servers.add(offlineServers);
        }

        if (pinotHelixResourceManager.hasRealtimeTable(tableName)) {
          JSONObject realtimeServers =
              getInstances(pinotHelixResourceManager.getServerInstancesForTable(tableName, TableType.REALTIME),
              TableType.REALTIME);
          servers.add(realtimeServers);
        }
      }
      ret.put("brokers", brokers);
      // TODO: should be servers (plural) but that may break client
      ret.put("server", servers);
      return ret.toString();
    } catch (Exception e) {
      LOGGER.error("Caught exception fetching instances for table ", e);
      metrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_INSTANCES_GET_ERROR, 1L);
      throw new WebApplicationException("Error fetching instances for table " + tableName, 500);
    }

  }
  private JSONObject getInstances(List<String> instanceList, TableType tableType)
      throws JSONException {
    JSONObject e = new JSONObject();
    // not sure how using enum toString will impact clients
    String typeStr = tableType==TableType.REALTIME ? "realtime" : "offline";
    e.put("tableType", typeStr);
    JSONArray a = new JSONArray();
    for (String ins : instanceList) {
      a.add(ins);
    }
    e.put("instances", a);
    return e;
  }
}
