package stream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    args.forEach { println(it) }
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "streams_onlab"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = args[0]
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.Integer().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

    val builder = StreamsBuilder()
    builder.stream<String, String>(args[1]).to(args[2])
    val topology = builder.build()
    println(topology)
    val streams = KafkaStreams(topology, props)
    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(Thread(){
        fun run() {
            streams.close()
            latch.countDown()
        }
    })
    try {
        streams.start()
        latch.await()
    } catch (e: Throwable) {
        exitProcess(1)
    }
    exitProcess(0)
}