package org.apache.kafka;

import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;

@ApplicationPath("kafka")
public class RestServerApplication extends ResourceConfig {

  public RestServerApplication() {
    register(RestServer.class);
  }
}