package org.acme.resource;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import org.acme.dto.UserConnectorConfigDto;
import org.acme.model.DataModelDTO;
import org.acme.resource.utili.ConnectorEnum;
import org.acme.resource.utili.EventResponseModel;
import org.acme.resource.utili.ResponseUtil;
import org.acme.service.UserConnectorService;
import org.bson.types.ObjectId;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static javax.ws.rs.core.Response.Status.NO_CONTENT;

@Path("/connector")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Blocking
public class UserConnectorResource {

    private UserConnectorService userConnectorService;

    @Inject
    public UserConnectorResource(UserConnectorService userConnectorService) {
        this.userConnectorService = userConnectorService;
    }

    @POST
    public Uni<EventResponseModel> addConnector(UserConnectorConfigDto userConnectorConfigDto) throws SQLException, ClassNotFoundException {
        if (userConnectorConfigDto.getConnectorType().equals(ConnectorEnum.SOURCE.toString()))
            return userConnectorService.addSourceConnector(userConnectorConfigDto);
        return userConnectorService.addSinkConnector(userConnectorConfigDto);
    }

    @GET
    @Path("/{name}")
    public ResponseUtil getConnectorByName(@PathParam("name") String name) {
        return userConnectorService.getConnectorByName(name);
    }

    @DELETE
    @Path("/{connector}")
    public ResponseUtil deleteConfig(@PathParam("connector") String connectorName) {
        return userConnectorService.deleteConfig(connectorName);
    }

    @PUT
    @Path("/{name}/config")
    public Uni<EventResponseModel> updateConfig(@PathParam("name") String name, UserConnectorConfigDto userConnectorConfigDto) {
        return userConnectorService.updateConnector(name, userConnectorConfigDto);
    }


    @GET
    @Path("/status/{connector}")
    public ResponseUtil getStatus(@PathParam("connector") String connector) {
        return userConnectorService.getStatus(connector);
    }

    @GET
    @Path("/topics/{connector}")
    public ResponseUtil getTopics(@PathParam("connector") String connector) {
        return userConnectorService.getTopics(connector);
    }

    @PUT
    @Path("/topics-reset/{connector}")
    public ResponseUtil resetTopics(@PathParam("connector") String connector) {
        return userConnectorService.resetTopics(connector);
    }

    @GET
    @Path("/all-connectors/{miniAppName}")
    public ResponseUtil getAllConnectorByAppId(@PathParam("miniAppName") String miniAppName) {
        ResponseUtil response = new ResponseUtil();
        List<UserConnectorConfigDto> byWorkSpaceId = userConnectorService.findminiAppName(miniAppName);
        if (byWorkSpaceId != null && !byWorkSpaceId.isEmpty()) {
            response.setStatusCode(Status.OK.getStatusCode());
            response.setData(byWorkSpaceId);
            response.setMessage("data is present...");
            return response;
        }
        response.setStatusCode(NO_CONTENT.getStatusCode());
        response.setMessage("no data found");
        return response;
    }

    @GET
    @Path("/all-connectors-state/{miniAppName}")
    public ResponseUtil getAllConnectorStateByMiniAppName(@PathParam("miniAppName") String miniAppName) {
        ResponseUtil response = new ResponseUtil();
        Map<String, String> byWorkSpaceId = userConnectorService.findStatus(miniAppName);
        if (byWorkSpaceId != null && !byWorkSpaceId.isEmpty()) {
            response.setStatusCode(Status.OK.getStatusCode());
            response.setData(byWorkSpaceId);
            response.setMessage("data is present...");
            return response;
        }
        response.setStatusCode(NO_CONTENT.getStatusCode());
        response.setMessage("no data found");
        return response;
    }

    @PUT
    @Path("/pause/{connector}")
    public ResponseUtil pauseConnector(@PathParam("connector") String connector) {
        return userConnectorService.pauseConnector(connector);
    }

    @PUT
    @Path("/resume/{connector}")
    public ResponseUtil resumeConnector(@PathParam("connector") String connector) {
        return userConnectorService.resumeConnector(connector);
    }

    @POST
    @Path("/table-structure")
    public ResponseUtil getTableStructure(UserConnectorConfigDto userConnectorConfigDto) {
        ResponseUtil response = new ResponseUtil();
        List<DataModelDTO> dataModelMapping = userConnectorService.getTableStructures(userConnectorConfigDto);
        if (dataModelMapping != null && !dataModelMapping.isEmpty()) {
            response.setData(dataModelMapping);
            response.setStatusCode(Status.OK.getStatusCode());
            return response;
        }
        response.setMessage("No data found for the given details..");
        response.setStatusCode(NO_CONTENT.getStatusCode());
        return response;
    }

    @GET
    @Path("/tables/{connectionName}")
    public List<String> getTables(@PathParam("connectionName") String connectionName) {
        ResponseUtil response = new ResponseUtil();
        List<String> tables = userConnectorService.getTables(connectionName);
        if (tables != null && !tables.isEmpty()) {
            return tables;
        }
        return tables;
    }

}
