package com.github.vadimyemelyanov

import com.github.vadimyemelyanov.config.ConnectorConfig
import com.github.vadimyemelyanov.config.ElasticConfig
import com.github.vadimyemelyanov.converters.SchemaConverter
import com.github.vadimyemelyanov.converters.StructConverter.convertElasticDocument2AvroStruct
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.elasticsearch.action.search.*
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

class ElasticSourceTask : SourceTask() {
    private lateinit var config: ConnectorConfig
    private lateinit var es: ElasticConfig
    private val stopping: AtomicBoolean = AtomicBoolean(false)
    private var indices: List<String> = emptyList()
    private var connectionRetryBackoff: Long = 0
    private var maxConnectionAttempts = 0
    private var topic: String? = null
    private var incrementingField: String? = null
    private var size = 0
    private var pollingMs = 0
    private val lastOffset: MutableMap<String, String> = HashMap()
    private val sentOffset: MutableMap<String, Int> = HashMap()
    private lateinit var sourceQueryBuilder: SourceQueryBuilder
    override fun version(): String {
        return ConnectorConfig.VERSION
    }

    override fun start(properties: Map<String, String>) {
        try {
            config = ConnectorConfig(properties)
            sourceQueryBuilder = SourceQueryBuilder(config)
        } catch (e: ConfigException) {
            throw ConnectException("Couldn't start ElasticSourceTask due to configuration error", e)
        }
        initEsConnection()
        indices = config.getString(ConnectorConfig.INDICES_CONFIG).split(",").toTypedArray().toList()
        if (indices.isEmpty()) {
            throw ConnectException(
                "Invalid configuration: each ElasticSourceTask must have at" +
                        " least one index assigned to it"
            )
        }
        topic = config.getString(ConnectorConfig.TOPIC_CONFIG)
        incrementingField = config.getString(ConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG)
        size = config.getString(ConnectorConfig.BATCH_MAX_ROWS_CONFIG).toInt()
        pollingMs = config.getString(ConnectorConfig.POLL_INTERVAL_MS_CONFIG).toInt()
    }

    private fun initEsConnection() {
        val esHost = config.getString(ConnectorConfig.ES_HOST_CONF)
        val esPort = config.getString(ConnectorConfig.ES_PORT_CONF).toInt()
        val esUser = config.getString(ConnectorConfig.ES_USER_CONF)
        val esPwd = config.getString(ConnectorConfig.ES_PWD_CONF)

        maxConnectionAttempts = config.getString(ConnectorConfig.CONNECTION_ATTEMPTS_CONFIG).toInt()
        connectionRetryBackoff = config.getString(ConnectorConfig.CONNECTION_BACKOFF_CONFIG).toLong()

        es = if (esUser == null || esUser.isEmpty()) {
            ElasticConfig(
                esHost,
                esPort,
                maxConnectionAttempts,
                connectionRetryBackoff
            )
        } else {
            ElasticConfig(
                esHost,
                esPort,
                esUser,
                esPwd,
                maxConnectionAttempts,
                connectionRetryBackoff
            )
        }
    }

    //will be called by connect with a different thread than the stop thread
    override fun poll(): List<SourceRecord> {
        val results: MutableList<SourceRecord> = ArrayList()
        indices.forEach { index: String ->
            if (!stopping.get()) {
                logger.info("fetching from  $index")
                val lastValue = fetchLastOffset(index)
                logger.info("found last value $lastValue")
                lastValue?.let { executeScroll(index, it, results) }
                logger.info("index $index total messages: ${sentOffset[index]}")
            }
        }
        if (results.isEmpty()) {
            logger.info("no data found, sleeping for {} ms", pollingMs)
            Thread.sleep(pollingMs.toLong())
        }
        return results
    }

    private fun fetchLastOffset(index: String): String? {
        //first we check in cache memory the last value
        if (lastOffset[index] != null) {
            return lastOffset[index]
        }

        //if cache is empty we check the framework
        if (context != null) {
            val offset = context.offsetStorageReader()
                .offset(
                    Collections.singletonMap(
                        INDEX,
                        index
                    )
                )
            if (offset != null) {
                return offset[POSITION] as String?
            }
        }

        //first execution, no last value
        //fetching the lower level directly to the elastic index (if it is not empty)
        val searchRequest = SearchRequest(index)
        val searchSourceBuilder = SearchSourceBuilder()
        searchSourceBuilder
            .query(sourceQueryBuilder.selectQuery)
            .sort(incrementingField, SortOrder.ASC)
        searchSourceBuilder.size(1) // only one record
        searchRequest.source(searchSourceBuilder)
        var searchResponse: SearchResponse? = null
        try {
            for (i in 0 until maxConnectionAttempts) {
                try {
                    searchResponse = es.getClient().search(searchRequest, RequestOptions.DEFAULT)
                    break
                } catch (e: IOException) {
                    logger.error("error in scroll")
                    Thread.sleep(connectionRetryBackoff)
                }
            }
            if (searchResponse == null) {
                throw RuntimeException("connection failed")
            }
            val hits = searchResponse.hits
            val totalShards = searchResponse.totalShards
            val successfulShards = searchResponse.successfulShards
            logger.info("total shard {}, successuful: {}", totalShards, successfulShards)
            val failedShards = searchResponse.failedShards
            for (failure in searchResponse.shardFailures) {
                // failures should be handled here
                logger.error("failed {}", failure)
            }
            if (failedShards > 0) {
                throw RuntimeException("failed shard in search")
            }
            val searchHits = hits.hits
            //here only one record
            for (hit in searchHits) {
                // do something with the SearchHit
                return hit.sourceAsMap[incrementingField].toString()
            }
        } catch (e: Exception) {
            logger.error("error fetching min value", e)
            return null
        }
        return null
    }

    private fun executeScroll(
        index: String,
        lastValue: String,
        results: MutableList<SourceRecord>
    ) {
        val searchRequest = SearchRequest(index)
        searchRequest.scroll(TimeValue.timeValueMinutes(1L))

        val searchSourceBuilder = SearchSourceBuilder()
        val rangeQuery: QueryBuilder = QueryBuilders.rangeQuery(incrementingField)
            .from(lastValue, lastOffset[index] == null)

        val queryBuilder = QueryBuilders
            .boolQuery()
            .must(rangeQuery)
            .must(sourceQueryBuilder.selectQuery)
        searchSourceBuilder
            .query(queryBuilder)
            .sort(incrementingField, SortOrder.ASC)
        searchSourceBuilder.size(1000)
        searchRequest.source(searchSourceBuilder)

        var searchResponse: SearchResponse? = null
        var scrollId: String? = null
        try {
            for (i in 0 until maxConnectionAttempts) {
                try {
                    searchResponse = es.getClient().search(searchRequest, RequestOptions.DEFAULT)
                    break
                } catch (e: IOException) {
                    logger.error("error in scroll")
                    Thread.sleep(connectionRetryBackoff)
                }
            }
            if (searchResponse == null) {
                throw RuntimeException("connection failed")
            }
            scrollId = searchResponse.scrollId
            var searchHits =
                parseSearchResult(index, results, searchResponse, scrollId)
            while (!stopping.get() &&
                searchHits != null &&
                searchHits.isNotEmpty() &&
                results.size < size
            ) {
                val scrollRequest = SearchScrollRequest(scrollId)
                scrollRequest.scroll(TimeValue.timeValueMinutes(1L))
                searchResponse = es.getClient().scroll(scrollRequest, RequestOptions.DEFAULT)
                scrollId = searchResponse.scrollId
                searchHits = parseSearchResult(index, results, searchResponse, scrollId)
            }
        } catch (t: Throwable) {
            logger.error("error", t)
        } finally {
            closeScrollQuietly(scrollId)
        }
    }

    private fun parseSearchResult(
        index: String,
        results: MutableList<SourceRecord>,
        searchResponse: SearchResponse,
        scrollId: Any
    ): Array<SearchHit>? {
        if (results.size > size) {
            return null //nothing to do: limit reached
        }
        val hits = searchResponse.hits
        val totalShards = searchResponse.totalShards
        val successfulShards = searchResponse.successfulShards
        logger.info("total shard {}, successful: {}", totalShards, successfulShards)
        logger.info("retrieved {}, scroll id : {}", hits, scrollId)
        val failedShards = searchResponse.failedShards
        for (failure in searchResponse.shardFailures) {
            // failures should be handled here
            logger.error("failed {}", failure)
        }
        if (failedShards > 0) {
            throw RuntimeException("failed shard in search")
        }
        val searchHits = hits.hits
        for (hit in searchHits) {
            // do something with the SearchHit
            val sourceAsMap = hit.sourceAsMap

            // Add the label in the configuration
            addLabel(sourceAsMap, config)
            val sourcePartition =
                Collections.singletonMap(
                    INDEX,
                    index
                )
            val sourceOffset =
                Collections.singletonMap(
                    POSITION,
                    sourceAsMap[incrementingField].toString()
                )
            val schema =
                SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, index)
            val struct =
                convertElasticDocument2AvroStruct(sourceAsMap, schema)

            //document key
            val key = java.lang.String.join("_", hit.index, hit.type, hit.id)
            val sourceRecord = SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,  //KEY
                Schema.STRING_SCHEMA,
                key,  //VALUE
                schema,
                struct
            )
            results.add(sourceRecord)
            lastOffset[index] = sourceAsMap[incrementingField].toString()
            sentOffset.merge(index, 1, Integer::sum)
        }
        return searchHits
    }

    private fun closeScrollQuietly(scrollId: String?) {
        val clearScrollRequest = ClearScrollRequest()
        clearScrollRequest.addScrollId(scrollId)
        var clearScrollResponse: ClearScrollResponse? = null
        try {
            clearScrollResponse = es.getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
        } catch (e: IOException) {
            logger.error("error in clear scroll", e)
        }
        val succeeded = clearScrollResponse != null && clearScrollResponse.isSucceeded
        logger.info("scroll {} cleared: {}", scrollId, succeeded)
    }

    //will be called by connect with a different thread than poll thread
    override fun stop() {
        stopping.set(true)
        es.closeConnection()
    }

    //utility method for testing
    fun setupTest(
        index: List<String>,
        esHost: String,
        esPort: Int,
        query: String
    ) {
        maxConnectionAttempts = 3
        connectionRetryBackoff = 1000
        es = ElasticConfig(
            esHost,
            esPort,
            maxConnectionAttempts,
            connectionRetryBackoff
        )
        sourceQueryBuilder = SourceQueryBuilder(query)
        indices = index
        if (indices.isEmpty()) {
            throw ConnectException(
                "Invalid configuration: each ElasticSourceTask must have at "
                        + "least one index assigned to it"
            )
        }
        topic = "test_topic"
        incrementingField = "@timestamp"
        size = 10000
        pollingMs = 1000
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ElasticSourceTask::class.java)
        private const val INDEX = "index"
        private const val POSITION = "position"
    }
}