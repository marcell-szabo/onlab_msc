package stream

import json.KafkaJsonDeserializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess
import json.KafkaJsonSerializer
import json.OtherJson
import json.SomeJson
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.ValueJoiner
import java.time.Duration

data class ResultJson(val component: String, val some_value: Int, val other_value: Double)

inline fun<reified T> create_serdes(serdeProps: HashMap<String, Any>): Serde<T> {
    val serializer = KafkaJsonSerializer<T>()
    serdeProps["JSONClass"] = T::class.java
    serializer.configure(serdeProps, false)
    val deserializer = KafkaJsonDeserializer<T>()
    serdeProps["JSONClass"] = T::class.java
    deserializer.configure(serdeProps, false)
    val jsonSerde = Serdes.serdeFrom(serializer, deserializer)
    //props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializer::class.java
    return jsonSerde
}
fun main(args: Array<String>) {
    args.forEach { println(it) }
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "streams_onlab"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = args[0]
    //props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass


    val serdeProps = HashMap<String, Any>()
    val someJsonSerde = create_serdes<SomeJson>(serdeProps)
    val otherJsonSerde = create_serdes<OtherJson>(serdeProps)

    val builder = StreamsBuilder()
    val someJsonData = builder.stream<String, SomeJson>(args[1], Consumed.with(Serdes.String(), someJsonSerde))
    val otherJsonData = builder.stream<String, OtherJson>(args[2], Consumed.with(Serdes.String(), otherJsonSerde))
    val resultJsonData = someJsonData.join(otherJsonData,
        { sj, oj -> println("runned join"); ResultJson(sj.component, sj.some_value, oj.other_value) },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)))

    resultJsonData.to(args[3])
    val stream = KafkaStreams(builder.build(), props)

    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(Thread(){
        fun run() {
            stream.close()
            latch.countDown()
        }
    })
    try {
        stream.start()
        latch.await()
    } catch (e: Throwable) {
        exitProcess(1)
    }
    exitProcess(0)
}
