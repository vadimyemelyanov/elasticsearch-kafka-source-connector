package com.github.vadimyemelyanov

import com.github.vadimyemelyanov.config.ConnectorConfig
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders

class SourceQueryBuilder {
    private var query: String

    constructor(query: String) {
        this.query = query
    }

    constructor(config: ConnectorConfig) {
        query = config.getString(ConnectorConfig.ES_QUERY)
    }

    val selectQuery: QueryBuilder
        get() = QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery(query))
}