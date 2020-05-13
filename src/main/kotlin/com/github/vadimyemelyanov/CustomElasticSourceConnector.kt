package com.github.vadimyemelyanov

import com.github.vadimyemelyanov.config.ConnectorConfig
import com.github.vadimyemelyanov.config.ConnectorConfig.Companion.VERSION
import com.github.vadimyemelyanov.config.ElasticConfig
import com.github.vadimyemelyanov.config.getLogger
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import org.elasticsearch.client.Request
import kotlin.collections.set
import kotlin.math.min

class CustomElasticSourceConnector : SourceConnector() {
    private val logger = getLogger(javaClass)
    private lateinit var config: ConnectorConfig
    private lateinit var elasticConnection: ElasticConfig
    private lateinit var configProperties: Map<String, String>

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> =
        elasticConnection.getClient()
            .lowLevelClient
            .performRequest(Request("GET", "_cat/indices"))
            .let { getIndexList(it, config.getString(ConnectorConfig.INDEX_PATTERN_CONFIG)) }
            .let {
                val numGroups = min(it.size, maxTasks)
                groupPartitions(it, numGroups)
            }
            .map {
                val taskProps: MutableMap<String, String> = HashMap(configProperties)
                taskProps[ConnectorConfig.INDICES_CONFIG] = it.joinToString(separator = ",")
                taskProps
            }


    override fun start(props: MutableMap<String, String>) {
        try {
            configProperties = props
            config = ConnectorConfig(props)
        } catch (e: ConfigException) {
            throw ConnectException(
                "Couldn't start ElasticSourceConnector due to configuration "
                        + "error", e
            )
        }

        val esHost = config.getString(ConnectorConfig.ES_HOST_CONF)
        val esPort = config.getString(ConnectorConfig.ES_PORT_CONF).toInt()
        val esUser = config.getString(ConnectorConfig.ES_USER_CONF)
        val esPwd = config.getString(ConnectorConfig.ES_PWD_CONF)
        val maxConnectionAttempts = config.getString(ConnectorConfig.CONNECTION_ATTEMPTS_CONFIG).toInt()
        val connectionRetryBackoff = config.getString(ConnectorConfig.CONNECTION_BACKOFF_CONFIG).toLong()

        if (esUser == null || esUser.isEmpty()) {
            elasticConnection = ElasticConfig(
                esHost,
                esPort,
                maxConnectionAttempts,
                connectionRetryBackoff
            )
        } else {
            elasticConnection = ElasticConfig(
                esHost,
                esPort,
                esUser,
                esPwd,
                maxConnectionAttempts,
                connectionRetryBackoff
            )
        }

        // Initial connection attempt
        if (!elasticConnection.testConnection()) {
            throw ConfigException("Cannot connect to source")
        }
    }

    override fun stop() {
        logger.info("stopping elastic source")
        elasticConnection.closeConnection()
    }

    override fun version() = VERSION

    override fun taskClass(): Class<out Task> = ElasticSourceTask::class.java

    override fun config(): ConfigDef {
        return ConnectorConfig.CONFIG_DEF
    }
}