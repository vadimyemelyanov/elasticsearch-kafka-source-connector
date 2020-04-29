package com.github.vadimyemelyanov.config

import mu.KotlinLogging
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import java.io.IOException

class ElasticConfig {
    private val logger = KotlinLogging.logger {}

    private var client: RestHighLevelClient
    private var connectionRetryBackoff: Long = 0
    private var maxConnectionAttempts = 0

    constructor(host: String, port: Int, maxConnectionAttempts: Int, connectionRetryBackoff: Long) {
        logger.info("elastic auth disabled")
        client = RestHighLevelClient(RestClient.builder(HttpHost(host, port)))
        this.maxConnectionAttempts = maxConnectionAttempts
        this.connectionRetryBackoff = connectionRetryBackoff
    }

    constructor(
        host: String,
        port: Int,
        user: String,
        pwd: String,
        maxConnectionAttempts: Int,
        connectionRetryBackoff: Long
    ) {
        logger.info("elastic auth enabled")
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(user, pwd)
        )
        client = RestHighLevelClient(
            RestClient.builder(
                HttpHost(host, port)
            ).setHttpClientConfigCallback { httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            }
        )
        this.maxConnectionAttempts = maxConnectionAttempts
        this.connectionRetryBackoff = connectionRetryBackoff
    }

    fun testConnection(): Boolean {
        for (i in 0 until maxConnectionAttempts) {
            try {
                client.ping(RequestOptions.DEFAULT)
                return true
            } catch (e: IOException) {
                logger.warn("cannot connect", e)
            }
            try {
                Thread.sleep(connectionRetryBackoff)
            } catch (ignored: InterruptedException) {
                //we should not be interrupted here..
            }
        }
        logger.error("failed {} connection trials", maxConnectionAttempts)
        return false
    }

    fun getClient(): RestHighLevelClient {
        return client
    }

    fun getConnectionRetryBackoff(): Long {
        return connectionRetryBackoff
    }

    fun getMaxConnectionAttempts(): Int {
        return maxConnectionAttempts
    }

    fun closeConnection() {
        try {
            client.close()
        } catch (e: IOException) {
            logger.error("error in close", e)
        }
    }
}