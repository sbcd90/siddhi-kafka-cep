package org.apache.kafka;

import org.apache.kafka.interfaces.SiddhiRuleProducer;
import org.apache.kafka.interfaces.SiddhiStreamsProducer;
import org.apache.kafka.utils.SiddhiRule;
import org.apache.kafka.utils.SiddhiStreamData;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

@Path("cep")
public class RestServer {

  @POST
  @Path("{streamId}/rule")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createRule(@PathParam("streamId") String streamId, SiddhiRule body) throws Exception {
    SiddhiRuleProducer ruleProducer = new SiddhiRuleProducer(body.getTopic(), body.getBootstrapServers());

    ruleProducer.createRule(streamId, body.getDefinitions(), body.getRule());
    return Response.created(new URI(streamId)).build();
  }

  @POST
  @Path("{streamId}/stream/data")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createData(@PathParam("streamId") String streamId, SiddhiStreamData body) throws Exception {
    SiddhiStreamsProducer streamsProducer =
      new SiddhiStreamsProducer(body.getTopic(), body.getBootstrapServers(), streamId);

    for (List<Object> data: body.getData()) {
      streamsProducer.produce(Arrays.asList(data));
    }
    streamsProducer.shutdown();
    return Response.created(new URI(streamId)).build();
  }
}