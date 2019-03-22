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
 * CamelSinkTask sends records to Camel
 */
public class CamelSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CamelSinkTask.class);

    private String taskName;
    private CamelContext camel;
    private String localUrl;
    private ProducerTemplate producer;

    @Override
    public String version() {
        return new CamelSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            taskName = props.get(CamelSinkConnector.NAME_CONFIG);
            log.info("Starting connector task {}", taskName);

            camel = new DefaultCamelContext();

            localUrl = "direct:" + taskName;
            String remoteUrl = props.get(CamelSinkConnector.COMPONENT_CONFIG) + "://" + props.get(CamelSinkConnector.ADDRESS_CONFIG) + "?" + props.get(CamelSinkConnector.OPTIONS_CONFIG);

            log.info("Creating Camel route from({}).to({})", localUrl, remoteUrl);
            camel.addRoutes(new RouteBuilder() {
                public void configure() {
                    from(localUrl).to(remoteUrl);
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
            producer.sendBodyAndHeader(localUrl, record.value(), "header", record.key());
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
