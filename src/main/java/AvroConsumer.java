import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 使用Avro作为反序列化器的消费者代码
 */
public class AvroConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "io.confluent.kafka.serializer.KafkaAvroDeserializer");
        String shemaUrl="";
        props.put("schema.registry.url", shemaUrl);
        String topic = "customerContacts";
        String brokers="";
        String groupId="";
        String url="";
        KafkaConsumer consumer = new KafkaConsumer(createConsumerConfig(brokers, groupId, url));
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println(topic);

        while (true) {
            ConsumerRecords<String,Customer> records = consumer.poll(100);
            for (ConsumerRecord<String, Customer> record : records) {
                System.out.println(record.value().getCustomerName());
            }
        }
    }

    public static Map<String, Object> createConsumerConfig(String brokers, String groupId, String url) {
        Map<String, Object> map = new HashMap<>();
        map.put("brokers", brokers);
        map.put("groupId",groupId);
        map.put("url", url);
        return map;
    }
}
