package com.noriakijr.kafka.tutorial1

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class ProducerDemoWithCallback

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ProducerDemoWithCallback::class.java)

    val bootstrapServers = "127.0.0.1:9092"

    // create producer properties
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    // create the producer
    val producer = KafkaProducer<String, String>(properties)

    for (i in 1..10) {
        // create a producer record
        val record = ProducerRecord<String, String>("first_topic", "hello world$i")

        // send data - asynchronous
        producer.send(record) { metadata, exception ->
            exception?.let { logger.error("Error while producing", it) }
                .run {
                    logger.info(
                        "Received message \n" +
                                "Topic: ${metadata.topic()}\n" +
                                "Partition: ${metadata.partition()}\n" +
                                "Offset: ${metadata.offset()}\n" +
                                "Timestamp: ${metadata.timestamp()}"
                    )
                }
        }
    }

    // flush data
    producer.flush()

    // flush and close producer
    producer.close()
}