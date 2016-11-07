package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.Utils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.json.JSONObject;


/**
 * API endpoint that returns the versions of Pinot components.
 */
@Api(tags = Constants.VERSION_TAG)
@Path("/version")
public class PinotVersionRestletResource {

  @GET
  @Produces(javax.ws.rs.core.MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Get version number of Pinot components")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success")})
  public String getVersionNumber() {
    JSONObject jsonObject = new JSONObject(Utils.getComponentVersions());
    return jsonObject.toString();
  }
}
