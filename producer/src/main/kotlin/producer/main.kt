package producer

import json.KafkaJsonDeserializer
import json.KafkaJsonSerializer
import json.OtherJson
import json.SomeJson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer


import java.util.Properties

class JSONProducer<V>(val props: Properties, private val appID: String, private val server: String, private val topic: String) {
    lateinit var producer: KafkaProducer<String, V>

    init {
        this.props[ProducerConfig.CLIENT_ID_CONFIG] = appID
        this.props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = server
        this.props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    }

    inline fun<reified T> create_serdes() {
        val serdeProps = HashMap<String, Any>()
        val serializer = KafkaJsonSerializer<T>()
        serdeProps["JSONClass"] = T::class.java
        serializer.configure(serdeProps, false)
        //val deserializer = KafkaJsonDeserializer<T>()
        //serdeProps["JSONClass"] = T::class.java
        //deserializer.configure(serdeProps, false)
        //val jsonSerde = Serdes.serdeFrom(serializer, deserializer)
        this.props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = serializer::class.java
        this.producer = KafkaProducer(this.props)
    }

    fun run(messages : Map<String, V>) {
        this.producer.use { producer ->  messages.forEach { it ->
           producer.send(ProducerRecord(this.topic, it.key, it.value)) { _: RecordMetadata, e: Exception? ->
               when(e) {
                   null -> println("produced record to topic ${this.topic}")
                   else -> e.printStackTrace()
               }
           }
        } }
        producer.flush()
    }
}


fun main(args: Array<String>) {
    val producer1 = JSONProducer<SomeJson>(Properties(), "onlab_producer1", args[0], args[1])
    producer1.create_serdes<SomeJson>()
    producer1.run(mapOf("macska" to SomeJson("nyavogo", 10), "kutya" to SomeJson("ugato", 11)))
    val producer2 = JSONProducer<OtherJson>(Properties(), "onlab_producer2", args[0], args[2])
    producer2.create_serdes<OtherJson>()
    producer2.run(mapOf("macska" to OtherJson("nyavogo", 10.0), "kutya" to OtherJson("purrogo", 11.0)))


}