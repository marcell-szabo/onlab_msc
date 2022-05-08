package stream

import json.KafkaJsonDeserializer
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
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.*
import java.io.Serializable
import java.time.Duration

data class ResultJson(val component: String, val some_value: Int, val other_value: Double): Serializable

inline fun <reified T> create_serdes(serdeProps: HashMap<String, Any>): Serde<T> {
    val serializer = KafkaJsonSerializer<T>()
    serdeProps["JSONClass"] = T::class.java
    serializer.configure(serdeProps, false)
    val deserializer = KafkaJsonDeserializer<T>()
    serdeProps["JSONClass"] = T::class.java

    deserializer.configure(serdeProps, false)
    //props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializer::class.java
    return Serdes.serdeFrom(serializer, deserializer)
}
fun main(args: Array<String>) {
    args.forEach { println(it) }
    val props = Properties()
    props[StreamsConfig.APPLICATION_ID_CONFIG] = "streams_onlab1"
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = args[0]
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
    props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 0
/*    val serdeProps = HashMap<String, Any>()
    val someJsonSerde = create_serdes<SomeJson>(serdeProps)
    val otherJsonSerde = create_serdes<OtherJson>(serdeProps)
    val resultJsonSerde = create_serdes<ResultJson>(serdeProps)

    val builder = StreamsBuilder()
    //builder.stream<String, String>(args[1]).print(Printed.toSysOut())
    val someJsonData = builder.stream(args[1], Consumed.with(Serdes.String(), someJsonSerde))
    //someJsonData.to(args[3], Produced.with(Serdes.String(), someJsonSerde))
    val otherJsonData = builder.stream(args[2], Consumed.with(Serdes.String(), otherJsonSerde))
    //someJsonData.to(args[3], Produced.with(Serdes.String(), someJsonSerde))
    val valueJoiner = ValueJoiner<SomeJson, OtherJson, ResultJson>{leftvalue, rightvalue -> ResultJson(leftvalue.component, leftvalue.some_value, rightvalue.other_value)}
    someJsonData.leftJoin(otherJsonData,
        valueJoiner,
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)),
        StreamJoined.with(Serdes.String(), someJsonSerde, otherJsonSerde)).to(args[3], Produced.with(Serdes.String(), resultJsonSerde))*/

    val builder = StreamsBuilder()
    val leftstring = builder.stream<String, String>(args[1], Consumed.with(Serdes.String(), Serdes.String()))
    val rightstring = builder.stream<String, String>(args[2], Consumed.with(Serdes.String(), Serdes.String()))
    //val resultstring = leftstring.leftJoin(rightstring, { leftvalue: String, rightvalue: String ->
    //    if (rightvalue.isEmpty()) leftvalue else leftvalue + rightvalue},
    //    JoinWindows.of(Duration.ofMinutes(5)).grace(Duration.ofSeconds(0)), StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))

    val resultstring = leftstring.merge(rightstring)
    resultstring.print(Printed.toSysOut())
    resultstring.to(args[3], Produced.with(Serdes.String(), Serdes.String()))
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
