package com.github.vadimyemelyanov

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Width

class ConnectorConfig : AbstractConfig {

    constructor(
        definition: ConfigDef,
        originals: MutableMap<*, *>,
        configProviderProps: MutableMap<String, *>,
        doLog: Boolean
    ) : super(definition, originals, configProviderProps, doLog)

    constructor(properties: Map<String, String>) : super(CONFIG_DEF, properties)
    constructor(definition: ConfigDef, originals: MutableMap<*, *>) : super(definition, originals)
    constructor(definition: ConfigDef, originals: MutableMap<*, *>, doLog: Boolean) : super(
        definition,
        originals,
        doLog
    )


    companion object {
        val CONFIG_DEF: ConfigDef = baseConfigDef()

        fun baseConfigDef(): ConfigDef {
            val config = ConfigDef()
            addDatabaseOptions(config)
            addModeOptions(config)
            addConnectorOptions(config)
            addLabelOptions(config)
            return config
        }

        private fun addDatabaseOptions(config: ConfigDef) {
            var orderInGroup = 0
            config.define(
                ES_HOST_CONF,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                ES_HOST_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_HOST_DISPLAY,
                listOf(INDEX_PATTERN_CONFIG)
            ).define(
                ES_PORT_CONF,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                ES_PORT_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_PORT_DISPLAY,
                listOf(INDEX_PATTERN_CONFIG)
            ).define(
                ES_QUERY,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                ES_QUERY_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_QUERY_DISPLAY
            ).define(
                ES_USER_CONF,
                ConfigDef.Type.STRING,
                null,
                Importance.HIGH,
                ES_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_USER_DISPLAY
            ).define(
                ES_PWD_CONF,
                ConfigDef.Type.STRING,
                null,
                Importance.HIGH,
                ES_PWD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                ES_PWD_DISPLAY
            ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                ConfigDef.Type.STRING,
                CONNECTION_ATTEMPTS_DEFAULT,
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
            ).define(
                CONNECTION_BACKOFF_CONFIG,
                ConfigDef.Type.STRING,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
            ).define(
                INDEX_PATTERN_CONFIG,
                ConfigDef.Type.STRING,
                INDEX_PATTERN_DEFAULT,
                Importance.MEDIUM,
                INDEX_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                INDEX_PATTERN_DISPLAY
            )
        }

        private fun addModeOptions(config: ConfigDef) {
            var orderInGroup = 0
            config.define(
                MODE_CONFIG,
                ConfigDef.Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.`in`(
                    MODE_UNSPECIFIED,
                    MODE_BULK,
                    MODE_TIMESTAMP,
                    MODE_INCREMENTING,
                    MODE_TIMESTAMP_INCREMENTING
                ),
                Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                MODE_DISPLAY, listOf(INCREMENTING_FIELD_NAME_CONFIG)
            ).define(
                INCREMENTING_FIELD_NAME_CONFIG,
                ConfigDef.Type.STRING,
                INCREMENTING_FIELD_NAME_DEFAULT,
                Importance.MEDIUM,
                INCREMENTING_FIELD_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                INCREMENTING_FIELD_NAME_DISPLAY
            )
        }

        private fun addConnectorOptions(config: ConfigDef) {
            var orderInGroup = 0
            config.define(
                POLL_INTERVAL_MS_CONFIG,
                ConfigDef.Type.STRING,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
            ).define(
                BATCH_MAX_ROWS_CONFIG,
                ConfigDef.Type.STRING,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
            ).define(
                TOPIC_CONFIG,
                ConfigDef.Type.STRING,
                Importance.HIGH,
                TOPIC_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_DISPLAY
            )
        }

        private fun addLabelOptions(config: ConfigDef) {
            var orderInGroup = 0
            config.define(
                LABEL_KEY,
                ConfigDef.Type.STRING,
                Importance.LOW,
                LABEL_KEY_DOC,
                LABELING_GROUP,
                ++orderInGroup,
                Width.LONG,
                LABEL_KEY_DISPLAY
            ).define(
                LABEL_VALUE,
                ConfigDef.Type.STRING,
                Importance.LOW,
                LABEL_VALUE_DOC,
                LABELING_GROUP,
                ++orderInGroup,
                Width.LONG,
                LABEL_VALUE_DISPLAY
            )
        }

        const val ES_HOST_CONF = "es.host"
        const val ES_HOST_DOC = "ElasticSearch host"
        const val ES_HOST_DISPLAY = "Elastic host"

        const val ES_PORT_CONF = "es.port"
        const val ES_PORT_DOC = "ElasticSearch port"
        const val ES_PORT_DISPLAY = "ElasticSearch port"

        const val ES_QUERY = "es.query"
        const val ES_QUERY_DOC = "ElasticSearch string query"
        const val ES_QUERY_DISPLAY = "ElasticSearch string query"

        const val ES_USER_CONF = "es.user"
        const val ES_USER_DOC = "Elasticsearch username"
        const val ES_USER_DISPLAY = "Elasticsearch username"

        const val ES_PWD_CONF = "es.password"
        const val ES_PWD_DOC = "Elasticsearch password"
        const val ES_PWD_DISPLAY = "Elasticsearch password"

        const val CONNECTION_ATTEMPTS_CONFIG = "connection.attempts"
        const val CONNECTION_ATTEMPTS_DOC = "Minimum number of attempts to retrieve a valid Elasticsearch connection."
        const val CONNECTION_ATTEMPTS_DISPLAY = "Elasticsearch connection attempts"
        const val CONNECTION_ATTEMPTS_DEFAULT = "3"

        const val CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms"
        const val CONNECTION_BACKOFF_DOC = "Backoff time in milliseconds between connection attempts."
        const val CONNECTION_BACKOFF_DISPLAY = "Elastic connection backoff in milliseconds"
        const val CONNECTION_BACKOFF_DEFAULT = "10000"

        const val POLL_INTERVAL_MS_CONFIG = "poll.interval.ms"
        const val POLL_INTERVAL_MS_DOC = ("Frequency in ms to poll for new data in "
                + "each index.")
        const val POLL_INTERVAL_MS_DEFAULT = "5000"
        const val POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)"

        const val BATCH_MAX_ROWS_CONFIG = "batch.max.rows"
        const val BATCH_MAX_ROWS_DOC =
            "Minimum number of documents to include in a single batch when polling for new data."
        const val BATCH_MAX_ROWS_DEFAULT = "10000"
        const val BATCH_MAX_ROWS_DISPLAY = "Max Documents Per Batch"

        const val MODE_UNSPECIFIED = ""
        const val MODE_BULK = "bulk"
        const val MODE_TIMESTAMP = "timestamp"
        const val MODE_INCREMENTING = "incrementing"
        const val MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing"

        const val INCREMENTING_FIELD_NAME_CONFIG = "incrementing.field.name"
        const val INCREMENTING_FIELD_NAME_DOC = "name of the strictly incrementing field to use to detect new records."
        const val INCREMENTING_FIELD_NAME_DEFAULT = ""
        const val INCREMENTING_FIELD_NAME_DISPLAY = "Incrementing Field Name"

        const val INDEX_PATTERN_CONFIG = "index.pattern"
        const val INDEX_PATTERN_DOC = "List of indices to include in copying."
        const val INDEX_PATTERN_DEFAULT = ""
        const val INDEX_PATTERN_DISPLAY = "Indices prefix Whitelist"


        const val TOPIC_CONFIG = "topic"
        const val TOPIC_DOC = "The Kafka topic to publish data"
        const val TOPIC_DISPLAY = "Kafka topic"

        const val LABEL_KEY = "label.key"
        const val LABEL_KEY_DOC = "The key of the label to add to each record"
        const val LABEL_KEY_DISPLAY = "Label key"

        const val LABEL_VALUE = "label.value"
        const val LABEL_VALUE_DOC = "The value of the label to add to each record"
        const val LABEL_VALUE_DISPLAY = "Label value"

        const val DATABASE_GROUP = "Elasticsearch"
        const val MODE_GROUP = "Mode"
        const val CONNECTOR_GROUP = "Connector"
        const val LABELING_GROUP = "Labeling"

        const val MODE_CONFIG = "mode"
        const val MODE_DOC = ""
        const val MODE_DISPLAY = "Index Incrementing field"

        const val INDICES_CONFIG = "es.indices"
    }
}