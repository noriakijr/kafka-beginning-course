package com.noriakijr.kafka.tutorial1

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

class ProducerDemoKeys

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ProducerDemoKeys::class.java)

    val bootstrapServers = "127.0.0.1:9092"

    // create producer properties
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

    // create the producer
    val producer = KafkaProducer<String, String>(properties)

    for (i in 0..9) {
        // create a producer record
        val topic = "first_topic"
        val value = "hello world$i"
        val key = "id_$i"

        val record = ProducerRecord<String, String>(topic, key, value)

        logger.info("Key: $key")
        // id_0 is going to partition 1
        // id_1 partition 0
        // id_2 partition 2
        // id_3 partition 0
        // id_4 partition 2
        // id_5 partition 2
        // id_6 partition 0
        // id_7 partition 2
        // id_8 partition 1
        // id_9 partition 2

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
        }.get() // block the send() to make it synchronous - don't do this in production!
    }

    // flush data
    producer.flush()

    // flush and close producer
    producer.close()
}