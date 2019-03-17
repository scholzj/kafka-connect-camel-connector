/*
 * Copyright 2018, Jakub Scholz
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package cz.scholz.kafka.connect.camelconnector;

import java.util.Collection;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CamelSinkTask log records using Logger
 */
public class CamelSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CamelSinkTask.class);

    private CamelContext camel;
    private ProducerTemplate producer;

    @Override
    public String version() {
        return new CamelSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            camel = new DefaultCamelContext();

            String camelUrl = props.get(CamelSinkConnector.COMPONENT_CONFIG) + "://" + props.get(CamelSinkConnector.ADDRESS_CONFIG) + "?" + props.get(CamelSinkConnector.OPTIONS_CONFIG);

            camel.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:sink-task").to(camelUrl);
                }
            });

            producer = camel.createProducerTemplate();

            camel.start();
        } catch (Exception e) {
            throw new ConnectException("Failed to create and start Camel context", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            producer.sendBodyAndHeader("direct:sink-task", record.value(), "header", record.key());
        }
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
