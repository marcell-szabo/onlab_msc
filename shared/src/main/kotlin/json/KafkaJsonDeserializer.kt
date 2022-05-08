package json


import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

class KafkaJsonDeserializer<T> : Deserializer<T> {
    lateinit var tClass: Class<T>



    @Suppress("UNCHECKED_CAST")
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        this.tClass = configs?.get("JSONClass") as Class<T>
        print(tClass)
    }

    override fun deserialize(topic: String?, data: ByteArray?): T? {
        if (data == null) {
            print("deserializing null data: ${data}")
            return null
        }
        val result: T
        try {
            print("deserializing")
            result = jacksonObjectMapper().readValue(data, tClass)
        } catch (e : Exception) {
            throw SerializationException(e)
        }
        return result
    }

    override fun close() {

    }

}