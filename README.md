# Kafka Connect Camel connector

**Kafka Connect Camel connector** is a plugin for Kafka Connect. 
It is using the [Apache Camel](http://camel.apache.org) inside to route messages to / from different sources / targets 

## Configuration options

TODO

## Examples

TODO

1. Copy the `camel-connector-0.0.1.jar` jar file to your KAfka Connect plugin directory
2. Create a connector instance Kafka Connect REST API:
```
curl -X POST -H "Content-Type: application/json" --data '{ "name": "camel-connector-test", "config": { "connector.class": "CamelSink", "tasks.max": "3", "topics": "kafka-test-apps", "level": "INFO" } }' http://localhost:8083/connectors
```