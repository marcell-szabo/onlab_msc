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
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.kstream.Produced
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
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

    val serdeProps = HashMap<String, Any>()
    val someJsonSerde = create_serdes<SomeJson>(serdeProps)
    val otherJsonSerde = create_serdes<OtherJson>(serdeProps)
    val resultJsonSerde = create_serdes<ResultJson>(serdeProps)

    val builder = StreamsBuilder()
    //builder.stream<String, String>(args[1]).print(Printed.toSysOut())
    val someJsonData = builder.stream(args[1], Consumed.with(Serdes.String(), someJsonSerde)).print(Printed.toSysOut())
    /*someJsonData.to(args[3], Produced.with(Serdes.String(), someJsonSerde))
    val otherJsonData = builder.stream<String, OtherJson>(args[2], Consumed.with(Serdes.String(), otherJsonSerde))
    someJsonData.join(otherJsonData,
        { sj, oj -> println("runned join"); ResultJson(sj.component, sj.some_value, oj.other_value) },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30))).to(args[3], Produced.with(Serdes.String(), resultJsonSerde))
*/
    val topology = builder.build()
    println(topology.describe())
    val stream = KafkaStreams(topology, props)

    val latch = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(Thread(){
        fun run() {
            stream.close()
            println("stream closed")
            latch.countDown()
        }
    })
    try {
        stream.start()
        println("stream started")
        latch.await()
    } catch (e: Throwable) {
        exitProcess(1)
    }
    exitProcess(0)
}
