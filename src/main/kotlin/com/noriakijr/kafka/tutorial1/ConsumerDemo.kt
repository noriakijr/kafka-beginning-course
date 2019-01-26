package com.noriakijr.kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerDemo

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ConsumerDemo::class.java)

    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "my-fourth-application"
    val topic = "first_topic"

    // create consumer configs
    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    // create consumer
    val consumer = KafkaConsumer<String, String>(properties)

    // subscribe consumer to our topic(s)
    consumer.subscribe(mutableListOf(topic))

    // poll for new data
    while (true) {
        val records = consumer.poll(Duration.ofMillis(100)) // it's new in Kafka 2.0.0
        records.forEach {
            logger.info("Key: ${it.key()}, Value: ${it.value()}")
            logger.info("Partition: ${it.partition()}, Offset: ${it.offset()}")
        }
    }
}