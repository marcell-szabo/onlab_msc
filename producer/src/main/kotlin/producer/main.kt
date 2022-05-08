package producer

import json.KafkaJsonDeserializer
import json.KafkaJsonSerializer
import json.OtherJson
import json.SomeJson
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer


import java.util.Properties
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

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
        while (true) {
            messages.forEach { it ->
                producer.send(ProducerRecord(this.topic, it.key, it.value), Callback { metadata, exception -> when (exception) {
                    null -> println("produced record to topic ${this.topic}")
                    else -> exception.printStackTrace()
                } })
            }

            if (Thread.interrupted()) {
                println("exiting")
                producer.flush()
                producer.close()
                break
            }
        }
    }
}

fun main(args: Array<String>) {
/*    val producer1 = JSONProducer<SomeJson>(Properties(), "onlab_producer1", args[0], args[1])
    producer1.create_serdes<SomeJson>()
    val t1 = thread(start = true) {
        producer1.run(mapOf("macska" to SomeJson("nyavogo", 10)*//*, "kutya" to SomeJson("ugato", 11)*//*))
    }
    val producer2 = JSONProducer<OtherJson>(Properties(), "onlab_producer2", args[0], args[2])
    producer2.create_serdes<OtherJson>()
    val t2 = thread(start = true){
        producer2.run(mapOf("macska" to OtherJson("nyavogo", 11.0)*//*, "kutya" to OtherJson("purrogo", 11.0)*//*))
    }
    print("sleep")
    Thread.sleep(300000)
    t1.interrupt()
    t2.interrupt()*/
    val props: Properties = Properties()
    props[ProducerConfig.CLIENT_ID_CONFIG] = "string-producer"
    props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = args[0]
    props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    val producer = KafkaProducer<String, String>(props)
    val t1 = thread(start = true) {
        while (true) {
            try{
                producer.send(ProducerRecord(args[1], "a", "a"), Callback { metadata, exception -> when (exception) {
                    null -> println("produced record to topic ${args[1]}")
                    else -> exception.printStackTrace()
                } })
                Thread.sleep(4000)
            } catch (e: Exception) {
                producer.flush()
                producer.close()
                break
            }
        }
    }
    val t2 = thread(start = true) {
        while (true) {
            try{
                producer.send(ProducerRecord(args[2], "a", "b"), Callback { metadata, exception -> when (exception) {
                    null -> println("produced record to topic ${args[2]}")
                    else -> exception.printStackTrace()
                } })
                Thread.sleep(4000)
            } catch (e: Exception) {
                producer.flush()
                producer.close()
                break
            }
        }
    }
    print("sleep")
    Thread.sleep(60000)
    t1.interrupt()
    t2.interrupt()
    t1.join()
    t2.join()
}
