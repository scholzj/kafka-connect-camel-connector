# Kafka Connect Camel connector

**Kafka Connect Camel connector** is a plugin for Kafka Connect. 
It is using the [Apache Camel](http://camel.apache.org) inside to route messages to / from different sources / targets 

Currently, only the Sink connector exists.

## Configuration options

| Option        | Description                                                         | Example                                |
| ------------- | ------------------------------------------------------------------- | -------------------------------------- |
| component     | The nae of the Apache Camel component which should be used          | `file`                                 | 
| address       | The address which will be used in the URL after the `://`           | `/tmp/output-files`                    |
| options       | List of options. The options should be in the form of `key1=value1` | `["autoCreate=true", "charset=utf-8"]` |

For example the examples used above would create a following Apache Camel endpoint: `file:///tmp/output-files?autoCreate=true&charset=utf-8`.

## Sink Examples

* Copy the `camel-connector-0.0.1-SNAPSHOT.jar` jar file and Apache Camel to your Kafka Connect plugin directory
* Create a connector instance Kafka Connect REST API:
```
curl -X POST -H "Content-Type: application/json" --data '{ "name": "camel-connector-test", "config": { "connector.class": "CamelSink", "tasks.max": "1", "topics": "mytopic", "component": "file", "address": "/tmp/camel" } }' http://localhost:8083/connectors
```
* Send some messages to the topic `mytopic`
* Check the files created in `/tmp/camel/`