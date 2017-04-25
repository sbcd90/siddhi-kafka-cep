siddhi-kafka-cep
================

- This is a simple CEP Engine leveraging the [Kafka Streams](http://docs.confluent.io/3.2.0/streams/) platform.

Key Features
============

- This project integrates Kafka Streams with [WSO2 Siddhi CEP Engine](https://github.com/wso2/siddhi) and allows the
users to leverage the best features of both platforms.

- The complex CEP patterns supported by Siddhi ([link](https://docs.wso2.com/display/CEP310/Queries)) can be used for
processing events.

Getting started
===============

- Compile the cep engine with maven.

```$xslt
mvn clean install
```

- Start the cep engine with the scripts in the `scripts` directory.

Architecture
============

[Design](CEP_design.PNG)

Api
===

- The Java api documentation to create a new Siddhi Rule can be found [here](siddhi-kafka-rule-manager/apidocs/index.html).

- The Java api documentation to publish & receive data from the CEP Engine can be found [here](siddhi-kafka-streams-executor/apidocs/index.html).

Examples
========

- Examples for using the Java api s can be found the [example](example) directory.

```$xslt

```

Using Kafka Producers & Consumers
=================================

- The standard Kafka Producers & Consumers can also be used to publish & retrieve
data out of this engine.



Rest Api
========

- A simple REST server is included with this project.

## Getting started with the Rest server

- Run the maven jetty plugin to start the server.

```$xslt
mvn jetty:run
```

## Payloads

- To create a Siddhi Rule:

  POST http://localhost:8080/cep/{streamId}/rule

  Body:
  
  ```$xslt
   {
   	"topic": "t1",
   	"bootstrapServers": "localhost:9092",
   	"definitions": ["define stream siddhiStream1 (symbol string, price double, volume long);"],
   	"rule": "@info(name = 'query1') from siddhiStream1[price < 20.0] select symbol, price, volume insert into outputStream"
   }
  ```
  
- To post streams of data:

  POST http://localhost:8085/cep/{streamId}/stream/data 
 
  Body:
  
  ```$xslt
    {
    	"topic": "t1",
    	"bootstrapServers": "localhost:9092",
    	"data": [["Rectangle", 19.0, 19], ["Square", 21.0, 21]]
    }
  ```

