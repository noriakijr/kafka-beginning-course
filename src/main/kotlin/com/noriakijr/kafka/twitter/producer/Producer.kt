package com.noriakijr.kafka.twitter.producer

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.event.Event
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import java.io.File
import java.io.FileInputStream
import java.util.*
import java.util.concurrent.LinkedBlockingQueue


class Producer

fun main(args: Array<String>) {
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    val msgQueue = LinkedBlockingQueue<String>(100000)
    val eventQueue = LinkedBlockingQueue<Event>(1000)

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = StatusesFilterEndpoint()
    // Optional: set up some followings and track terms
    val followings = Lists.newArrayList(1234L, 566788L)
    val terms = Lists.newArrayList("twitter", "api")
    hosebirdEndpoint.followings(followings)
    hosebirdEndpoint.trackTerms(terms)

    // Get keys from properties file
    val fileKeys = FileInputStream("/keys/twitter-kafka-for-beginners-course.properties")
    val properties = Properties()
    properties.load(fileKeys)

    val consumerKey = properties.getProperty("twitter.consumer-key")
    val consumerSecret = properties.getProperty("twitter.consumer-secret-key")
    val token = properties.getProperty("twitter.token")
    val secret = properties.getProperty("twitter.token-secret")

    // These secrets should be read from a config file
    val hosebirdAuth = OAuth1(consumerKey, consumerSecret, token, secret)

    val builder = ClientBuilder()
        .name("Hosebird-Client-01")                 // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(StringDelimitedProcessor(msgQueue))
        .eventMessageQueue(eventQueue)                      // optional: use this if you want to process client events

    val hosebirdClient = builder.build()
    // Attempts to establish a connection.
    hosebirdClient.connect()

    while (!hosebirdClient.isDone) {
        println(msgQueue.take())
    }
}