package com.github.vadimyemelyanov

import org.elasticsearch.client.Response
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.*
import kotlin.streams.toList


fun groupPartitions(
    currentIndices: List<String>,
    numGroups: Int
): List<MutableList<String>> {
    val result: MutableList<MutableList<String>> =
        ArrayList(numGroups)
    for (i in 0 until numGroups) {
        result.add(ArrayList())
    }
    for (i in currentIndices.indices) {
        result[i % numGroups].add(currentIndices[i])
    }
    return result
}

fun getIndexList(
    indicesReply: Response,
    indicesPattern: String
): List<String> =
    BufferedReader(InputStreamReader(indicesReply.entity.content)).use { reader ->
        reader.lines()
            .map { it.split("\\s+").toTypedArray()[2] }
            .filter { it == indicesPattern }
            .toList()
    }


//not all elastic names are valid avro name
fun filterAvroName(elasticName: String): String {
    return elasticName.replace("[^a-zA-Z0-9]".toRegex(), "")
}

fun filterAvroName(prefix: String, elasticName: String?): String {
    return if (elasticName == null) prefix else prefix + elasticName.replace("[^a-zA-Z0-9]".toRegex(), "")
}

/**
 * Adds the label (key, value) to the record
 *
 * @param sourceAsMap The ElasticSearch result as a Map
 * @param config The configuration where the label is defined
 * @return The labeled struct
 */
fun addLabel(
    sourceAsMap: MutableMap<String, Any>,
    config: ConnectorConfig
): Map<String, Any> {
    val keyString: String = config.getString(ConnectorConfig.LABEL_KEY)
    val valueString: String = config.getString(ConnectorConfig.LABEL_VALUE)
    if (!keyString.isEmpty() && !valueString.isEmpty()) {
        sourceAsMap[keyString] = valueString
    }
    return sourceAsMap
}