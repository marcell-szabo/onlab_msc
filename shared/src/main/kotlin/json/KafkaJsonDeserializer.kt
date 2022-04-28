package json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.*
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import kotlin.reflect.KClass
import kotlin.reflect.KType

class KafkaJsonDeserializer<T> : Deserializer<T> {
    val objectmapper = ObjectMapper()
    lateinit var tClass: Class<T>

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        this.tClass = configs?.get("JSONClass") as Class<T>
        print(tClass)
    }

    override fun deserialize(topic: String?, data: ByteArray?): T? {
        if (data == null)
            return null
        val result: T
        try {
            result = objectmapper.readValue(data, this.tClass)
        } catch (e : Exception) {
            throw SerializationException(e)
        }
        return result
    }

    override fun close() {

    }

}