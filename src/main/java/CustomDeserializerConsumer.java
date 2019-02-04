import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * 自定义反序列化器消费者代码
 */
public class CustomDeserializerConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","CustomerDeserializer");
        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("customerCountries"));

        while (true) {
            ConsumerRecords<String, Customer> records = consumer.poll(100);
            for (ConsumerRecord<String, Customer> record : records) {
                System.out.println(record);
            }
        }

    }
}
