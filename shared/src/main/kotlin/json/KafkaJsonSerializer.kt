package json

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

class KafkaJsonSerializer<T>() : Serializer<T> {
    val objectmapper = ObjectMapper()
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serialize(topic: String?, data: T): ByteArray? {
        if (data == null)
            return null
        try {
            return objectmapper.writeValueAsBytes(data)
        } catch (e: java.lang.Exception) {
            throw SerializationException("Error serializing JSON", e)
        }
    }

    override fun close() {
        super.close()
    }

}