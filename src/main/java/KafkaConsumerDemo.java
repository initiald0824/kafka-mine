import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者
 */
public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Map<String, Integer> custCountryMap=new HashMap<>();

        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("group.id", "countryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("customerCountries"));
        try {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s,%s,%s,%s", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                int updateCount=1;
                if (custCountryMap.containsValue(record.value())) {
                    updateCount=custCountryMap.get(record.value())+1;
                }
                custCountryMap.put(record.value(), updateCount);
                System.out.println(custCountryMap);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
