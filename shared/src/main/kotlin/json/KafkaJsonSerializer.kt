package json

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.jsonMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

class KafkaJsonSerializer<T>() : Serializer<T> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}

    override fun serialize(topic: String?, data: T): ByteArray? {
        if (data == null) return null
        try {
            return jacksonObjectMapper().writeValueAsString(data).toByteArray(StandardCharsets.UTF_8)
        } catch (e: java.lang.Exception) {
            throw SerializationException("Error serializing JSON", e)
        }
    }

    override fun close() {}
}