package consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import java.util.Properties

fun main(args: Array<String>) {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = args[0]
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java


}