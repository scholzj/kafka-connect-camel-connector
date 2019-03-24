/*
 * Copyright 2018, Jakub Scholz
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package cz.scholz.kafka.connect.camelconnector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CamelSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(CamelSourceConnector.class);

    public static final String NAME_CONFIG = "name";
    public static final String COMPONENT_CONFIG = "component";
    public static final String ADDRESS_CONFIG = "address";
    public static final String OPTIONS_CONFIG = "options";
    public static final String TOPIC_CONFIG = "topic";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(COMPONENT_CONFIG, Type.STRING, Importance.HIGH, "Name of the Apache Camel component which will be used to route messages. (component://address?option1=value1&options2=value2)")
        .define(ADDRESS_CONFIG, Type.STRING, Importance.HIGH, "The address which will be used to configure the component. (component://address?option1=value1&options2=value2)")
        .define(OPTIONS_CONFIG, Type.LIST, Collections.EMPTY_LIST, Importance.HIGH, "List of options for configuring the component. The list should contain key-value pairs. E.g. `[\"option1=key1\", \"option2=key2\"]`.")
        .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, "The topic to publish data to");

    private String connectorName;
    private String component;
    private String address;
    private List<String> options;
    private String topic;

    @Override
    public String version() {
        return "0.0.1-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Connector config keys: {}", String.join(", ", props.keySet()));

        connectorName = props.get(NAME_CONFIG);
        log.info("Starting connector {}", connectorName);

        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        component = parsedConfig.getString(COMPONENT_CONFIG);
        address = parsedConfig.getString(ADDRESS_CONFIG);
        options = parsedConfig.getList(OPTIONS_CONFIG);
        topic = parsedConfig.getString(TOPIC_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CamelSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        Map<String, String> config = new HashMap<>(4);
        config.put(COMPONENT_CONFIG, component);
        config.put(ADDRESS_CONFIG, address);
        config.put(OPTIONS_CONFIG, String.join("&", options));
        config.put(TOPIC_CONFIG, topic);

        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(config);
            taskConfig.put(NAME_CONFIG, connectorName + "-" + i);

            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
