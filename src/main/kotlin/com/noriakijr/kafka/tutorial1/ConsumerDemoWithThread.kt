package com.noriakijr.kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CountDownLatch

class ConsumerDemoWithThread

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ConsumerDemoWithThread::class.java)

    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "my-sixth-application"
    val topic = "first_topic"

    // latch for dealing with multiple threads
    val latch = CountDownLatch(1)

    // create the consumer runnable
    logger.info("Creating the consumer thread")
    val myConsumerRunnable: Runnable = ConsumerRunnable(bootstrapServers, groupId, topic, latch)

    // start the thread
    val myThread = Thread(myConsumerRunnable)
    myThread.start()

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        logger.info("Caught shutdown hook")
        (myConsumerRunnable as ConsumerRunnable).shutdown()

        try {
            latch.await()
        } catch (e: InterruptedException) {
            e.printStackTrace()
        }
        logger.info("Application has exited")
    })

    try {
        latch.await()
    } catch (e: InterruptedException) {
        logger.error("Application got interrupted", e)
    } finally {
        logger.info("Application is closing")
    }
}

internal class ConsumerRunnable(bootstrapServers: String,
                                groupId: String,
                                topic: String,
                                private val latch: CountDownLatch) : Runnable {

    private val logger = LoggerFactory.getLogger(ConsumerRunnable::class.java)
    private var consumer: KafkaConsumer<String, String>

    init {
        // create consumer configs
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        // create consumer
        consumer = KafkaConsumer(properties)

        // subscribe consumer to our topic(s)
        consumer.subscribe(mutableListOf(topic))
    }

    override fun run() {
        // poll for new data
        try {
            while (true) {
                val records = consumer.poll(Duration.ofMillis(100)) // it's new in Kafka 2.0.0
                records.forEach {
                    logger.info("Key: ${it.key()}, Value: ${it.value()}")
                    logger.info("Partition: ${it.partition()}, Offset: ${it.offset()}")
                }
            }
        } catch (e: WakeupException) {
            logger.info("Received shutdown signal!")
        } finally {
            consumer.close()
            // tell our main code we're done with the consumer
            latch.countDown()
        }
    }

    fun shutdown() {
        // the wakeup() method is a special method to interrupt consumer.poll()
        // it will throw the exception WakeUpException
        consumer.wakeup()
    }

}