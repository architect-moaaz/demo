package org.acme.service;

import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.json.simple.JSONObject;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@ApplicationScoped
@Path("/connectors")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RegisterRestClient(configKey="connector-api")
public interface ConnectorService {

    @POST
    Response addConnector(JSONObject connector);

    @GET
    @Path("/{connector}/status")
    JSONObject findStatus(@PathParam("connector") String connector);

    @DELETE
    @Path("/{name}")
    Response deleteConnector(@PathParam("name") String name);

    @PUT
    @Path("/{name}/config")
    Response updateConnector(@PathParam("name") String name, JSONObject connector);

    @GET
    @Path("/{name}/config")
    JSONObject getConnector(@PathParam("name") String name);

    @PUT
    @Path("/{connector}/pause")
    void pauseConnector(@PathParam("connector") String connector);

    @PUT
    @Path("/{connector}/resume")
    void resumeConnector(@PathParam("connector") String connector);

    @GET
    @Path("/{connector}/topics")
    JSONObject getTopics(@PathParam("connector") String connector);

    @PUT
    @Path("/{connector}/topics/reset")
    JSONObject resetTopics(@PathParam("connector") String connector);
}
