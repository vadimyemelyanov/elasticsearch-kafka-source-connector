package com.github.vadimyemelyanov.converters

import com.github.vadimyemelyanov.filterAvroName
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import java.util.stream.Collectors

object StructConverter {
    fun convertElasticDocument2AvroStruct(doc: Map<String, Any>, schema: Schema): Struct {
        val struct = Struct(schema)
        convertDocumentStruct(
            "",
            doc,
            struct,
            schema
        )
        return struct
    }

    private fun convertDocumentStruct(prefixName: String, doc: Map<String, Any>, struct: Struct, schema: Schema) {
        doc.keys.forEach { key: String ->
            when (val value = doc[key]) {
                is String, Int, Long, Double, Boolean, Float -> struct.put(
                    filterAvroName(
                        key
                    ), value)
                is List<*> -> buildListStruct(
                    value,
                    struct,
                    key,
                    schema,
                    prefixName
                )
                is Set<*> -> buildListStruct(
                    value.toList(),
                    struct,
                    key,
                    schema,
                    prefixName
                )
                is Map<*, *> -> {
                    val nestedStruct = Struct(schema.field(
                        filterAvroName(
                            key
                        )
                    ).schema())
                    convertDocumentStruct(
                        filterAvroName(prefixName, key) + ".",
                        value as Map<String, Any>,
                        nestedStruct,
                        schema.field(filterAvroName(key)).schema()
                    )
                    struct.put(filterAvroName(key), nestedStruct)
                }
                else -> throw RuntimeException("type not supported $key")
            }
        }
    }

    private fun buildListStruct(
        value: List<*>,
        struct: Struct,
        key: String,
        schema: Schema,
        prefixName: String
    ) {
        if (value.isNotEmpty()) {
            //assuming that every item of the list has the same schema
            val item = value[0]!!
            struct.put(filterAvroName(key), java.util.ArrayList<Any>())
            when (item) {
                is Map<*, *> -> {
                    val array = value
                        .stream()
                        .map {
                            val nestedStruct =
                                Struct(schema.field(
                                    filterAvroName(
                                        prefixName,
                                        key
                                    )
                                ).schema().valueSchema())
                            convertDocumentStruct(
                                filterAvroName(prefixName, key) + ".",
                                it as Map<String, Any>,
                                nestedStruct,
                                schema.field(filterAvroName(key)).schema()
                                    .valueSchema()
                            )
                            nestedStruct
                        }
                        .collect(Collectors.toCollection<Any, java.util.ArrayList<Any>> { ArrayList() }) as List<Struct>
                    struct.put(filterAvroName(key), array)
                }
                else -> {
                    struct.getArray<Any>(filterAvroName(key)).addAll(value)
                }
            }
        }
    }
}