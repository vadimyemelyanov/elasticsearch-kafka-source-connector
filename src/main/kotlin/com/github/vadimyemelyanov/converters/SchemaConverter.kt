package com.github.vadimyemelyanov.converters

import com.github.vadimyemelyanov.filterAvroName
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder

object SchemaConverter {
    fun convertElasticMapping2AvroSchema(
        doc: Map<String, Any>,
        name: String?
    ): Schema {
        val schemaBuilder = SchemaBuilder.struct().name(
            filterAvroName("", name)
        ) //characters not valid for avro schema name
        convertDocumentSchema(
            "",
            doc,
            schemaBuilder
        )
        return schemaBuilder.build()
    }

    private fun convertDocumentSchema(
        prefixName: String,
        doc: Map<String, Any>,
        schemaBuilder: SchemaBuilder
    ) {
        doc.keys.forEach { key: String ->
            when (val value = doc[key]) {
                is String -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_STRING_SCHEMA)
                is Int -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_INT32_SCHEMA)
                is Long -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_INT64_SCHEMA)
                is Float -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_FLOAT32_SCHEMA)
                is Double -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_FLOAT64_SCHEMA)
                is Boolean -> schemaBuilder.field(filterAvroName(key), Schema.OPTIONAL_BOOLEAN_SCHEMA)
                is Set<*> -> {
                    buildListSchema(
                        value.toList(),
                        schemaBuilder,
                        key,
                        prefixName
                    )
                }
                is List<*> -> {
                    buildListSchema(
                        value,
                        schemaBuilder,
                        key,
                        prefixName
                    )
                }
                is Map<*, *> -> {
                    val nestedSchema =
                        SchemaBuilder.struct().name(
                            filterAvroName(
                                prefixName,
                                key
                            )
                        ).optional()
                    convertDocumentSchema(
                        filterAvroName(prefixName, key) + ".",
                        value as Map<String, Any>,
                        nestedSchema
                    )
                    schemaBuilder.field(filterAvroName(key), nestedSchema.build())
                }
                else -> throw RuntimeException("type not supported $key")
            }
        }
    }

    private fun buildListSchema(
        value: List<*>,
        schemaBuilder: SchemaBuilder,
        key: String,
        prefixName: String
    ) {
        if (value.isNotEmpty()) {
            when (val item = value[0]!!) {
                is String -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Int -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Long -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Float -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Double -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Boolean -> {
                    schemaBuilder.field(
                        filterAvroName(key), SchemaBuilder.array(SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
                            .optional()
                            .build()
                    ).build()
                }
                is Map<*, *> -> {
                    val nestedSchema = SchemaBuilder.struct()
                        .name(filterAvroName(prefixName, key))
                        .optional()
                    convertDocumentSchema(
                        filterAvroName(prefixName, key) + ".",
                        item as Map<String, Any>,
                        nestedSchema
                    )
                    schemaBuilder.field(filterAvroName(key), SchemaBuilder.array(nestedSchema.build()))
                }
                else -> throw RuntimeException("error in converting list: type not supported")
            }
        }
    }
}