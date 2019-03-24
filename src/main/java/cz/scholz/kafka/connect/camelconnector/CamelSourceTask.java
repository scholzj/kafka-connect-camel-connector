/*
 * Copyright 2018, Jakub Scholz
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package cz.scholz.kafka.connect.camelconnector;

import org.apache.camel.CamelContext;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CamelSourceTask get records from Camel
 */
public class CamelSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(CamelSourceTask.class);

    private String taskName;
    private String topic;
    private CamelContext camel;
    private String localUrl;
    private ConsumerTemplate consumer;

    @Override
    public String version() {
        return new CamelSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            taskName = props.get(CamelSourceConnector.NAME_CONFIG);
            topic = props.get(CamelSourceConnector.TOPIC_CONFIG);
            log.info("Starting connector task {}", taskName);

            camel = new DefaultCamelContext();

            localUrl = "direct:" + taskName;
            String remoteUrl = props.get(CamelSourceConnector.COMPONENT_CONFIG) + "://" + props.get(CamelSourceConnector.ADDRESS_CONFIG) + "?" + props.get(CamelSourceConnector.OPTIONS_CONFIG);

            log.info("Creating Camel route from({}).to({})", remoteUrl, localUrl);
            camel.addRoutes(new RouteBuilder() {
                public void configure() {
                    from(remoteUrl).to(localUrl);
                }
            });

            consumer = camel.createConsumerTemplate();

            camel.start();
        } catch (Exception e) {
            throw new ConnectException("Failed to create and start Camel context", e);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> records = new ArrayList<>();

        while (true)    {
            Exchange ex = consumer.receiveNoWait(localUrl);

            if (ex != null) {
                log.info("Received exchange with");
                log.info("\t from endpoint: {}", ex.getFromEndpoint());
                log.info("\t exchange id: {}", ex.getExchangeId());
                log.info("\t message id: {}", ex.getMessage().getMessageId());
                log.info("\t message body: {}", ex.getMessage().getBody());
                Map<String, Object> sourcePartition = Collections.singletonMap("filename", ex.getFromEndpoint().toString());
                Map<String, Object> sourceOffset = Collections.singletonMap("position", ex.getExchangeId());
                SourceRecord record = new SourceRecord(sourcePartition, sourceOffset, topic, Schema.BYTES_SCHEMA, ex.getMessage().getBody());
                records.add(record);
                consumer.doneUoW(ex);
            } else {
                break;
            }
        }

        return records;
    }

    @Override
    public void stop() {
        try {
            camel.stop();
        } catch (Exception e) {
            throw new ConnectException("Failed to stop Camel context", e);
        }
    }
}
