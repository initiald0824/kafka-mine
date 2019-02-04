import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 使用一般的Avro对象，只需要提供schema
 */
public class AvroObject {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        String shemaUrl = "";
        props.put("schema.registry.url", shemaUrl);
        String shemaString = "jsonString{}";
        Producer<String, Customer> producer = new KafkaProducer<String, Customer>(props);

        int customers=100;
        for (int nCustomers = 0; nCustomers<customers; nCustomers++) {
            String name="name";

            Customer customer=new Customer(1,name);

            ProducerRecord<String, Customer> data = new ProducerRecord<String, Customer>("customerContacts", name, customer);
            producer.send(data);
        }

    }
}
