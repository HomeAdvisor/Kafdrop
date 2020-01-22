# Kafdrop

Kafdrop is a UI for monitoring Apache Kafka clusters. The tool displays information such as brokers, topics, partitions, and even lets you view messages. It is a light weight application that runs on Spring Boot and requires very little configuration.

## Requirements

* Java 8
* Kafka 2.0 or later (might work with earlier versions)
* Zookeeper (3.4.5 or later)

Optional, additional integration:

* Schema Registry

## Building

After cloning the repository, building should just be a matter of running a standard Maven build:

```
$ mvn clean package
```

## Running Stand Alone

The build process creates an executable JAR file.  

```
java -jar ./target/kafdrop-<version>.jar --zookeeper.connect=<host:port>,<host:port>,... --kafka.brokers=<host:port>,<host:port>,...
```

Then open a browser and navigate to http://localhost:9000. The port can be overridden by adding the following config:

```
    --server.port=<port>
```

Additionally, you can optionally configure a schema registry connection with:
```
    --schemaregistry.connect=http://localhost:8081
```

Finally, a default message format (e.g. to deserialize Avro messages) can optionally be configured as follows:
```
    --message.format=AVRO
```
Valid format values are "DEFAULT" and "AVRO". This setting can also be configured at the topic level via dropdown when viewing messages.

## Running with Docker

Note for Mac Users: You need to convert newline formatting of the kafdrop.sh file *before* running this command:

```
    dos2unix src/main/docker/*
```

The following maven command will generate a Docker image:

```
    mvn clean package assembly:single docker:build
```


Once the build finishes you can launch the image as follows:

```
    docker run -d -p 9000:9000 -e ZOOKEEPER_CONNECT=<host:port,host:port> kafdrop
```

And access the UI at http://localhost:9000.

## Configuration Options

| Option                                | Default | Description                                                            | 
| ------------------------------------- | ------- | ---------------------------------------------------------------------- |
| kafka.truststoreLocation              |         | Location of the truststore used for secure connections                 |
| kafka.keystoreLocation                |         | Location of the keystore used for secure connections                   |
| kafka.additionalProperties.<property> |         | Additional properties to pass to the Admin or Consumer clients         |
| kafka.adminPool.minIdle               | 0       | Minimum number of unused admin clients to keep open                    |
| kafka.adminPool.maxIdle               | 8       | Maximum number of unused admin clients to keep open                    |
| kafka.adminPool.maxTotal              | 8       | Maximum number of admin clients to have open at one time               |
| kafka.adminPool.maxWaitMillis         | -1      | Milliseconds to wait for an admin client from the pool (-1 == forever) |
| kafka.consumerPool.minIdle            | 0       | Minimum number of unused consumer clients to keep open                 |
| kafka.consumerPool.maxIdle            | 8       | Maximum number of unused consumer clients to keep open                 |
| kafka.consumerPool.maxTotal           | 8       | Maximum number of consumer clients to have open at one time            |
| kafka.consumerPool.maxWaitMillis      | -1      | Millis to wait for a consumer client from the pool (-1 == forever)     |

## Kafka APIs

Starting with version 2.0.0, Kafdrop offers a set of Kafka APIs that mirror the existing HTML views. Any existing endpoint can be returned as JSON by simply setting the *Accept : application/json header*. There are also two endpoints that are JSON only:

    /topic : Returns array of all topic names
    /topic/{topicName}/{consumerId} : Return partition offset and lag details for a specific topic and consumer.

## Swagger

To help document the Kafka APIs, Swagger has been included. The Swagger output is available by default at the following Kafdrop URL:

    /v2/api-docs
    
However this can be overridden with the following configuration:

    springfox.documentation.swagger.v2.path=/new/swagger/path

Currently only the JSON endpoints are included in the Swagger output; the HTML views and Spring Boot debug endpoints are excluded.

You can disable Swagger output with the following configuration:

    swagger.enabled=false

## CORS Headers

Starting in version 2.0.0, Kafdrop sets CORS headers for all endpoints. You can control the CORS header values with the following configurations:

    cors.allowOrigins (default is *)
    cors.allowMethods (default is GET,POST,PUT,DELETE)
    cors.maxAge (default is 3600)
    cors.allowCredentials (default is true)
    cors.allowHeaders (default is Origin,Accept,X-Requested-With,Content-Type,Access-Control-Request-Method,Access-Control-Request-Headers,Authorization)
    
You can also disable CORS entirely with the following configuration:

    cors.enabled=false
