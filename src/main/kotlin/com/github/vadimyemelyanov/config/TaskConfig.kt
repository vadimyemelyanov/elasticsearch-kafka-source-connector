package com.github.vadimyemelyanov.config

import org.apache.kafka.common.config.ConfigDef

class TaskConfig(props: Map<String, String>) : ConnectorConfig(config, props) {
    companion object {
        var config: ConfigDef = baseConfigDef()
            .define(
                INDICES_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                INDICES_CONFIG
            )
    }
}