package com.noriakijr.kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerDemoAssignSeek

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek::class.java)

    val bootstrapServers = "127.0.0.1:9092"
    val topic = "first_topic"

    // create consumer configs
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    // create consumer
    val consumer = KafkaConsumer<String, String>(properties)

    // assign and seek are mostly used to replay data or fetch a specific message
    // assign
    val partitionToReadFrom = TopicPartition(topic, 0)
    val offsetToReadFrom = 15L
    consumer.assign(mutableListOf(partitionToReadFrom))

    // seek
    consumer.seek(partitionToReadFrom, offsetToReadFrom)

    val numberOfMessagesToRead = 5
    var numberOfMessagesReadSoFar = 0

    // poll for new data
    run loop@{
        while (true) {
            val records = consumer.poll(Duration.ofMillis(100)) // it's new in Kafka 2.0.0
            records.forEach {
                numberOfMessagesReadSoFar++
                logger.info("Key: ${it.key()}, Value: ${it.value()}")
                logger.info("Partition: ${it.partition()}, Offset: ${it.offset()}")
                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    return@loop // to exit the while loop
                }

            }
        }
    }

    logger.info("Exiting the application")

}