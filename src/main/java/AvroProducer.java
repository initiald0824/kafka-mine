import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 使用Avro作为序列化器的生产者代码
 */
public class AvroProducer {

    private static Properties kafkaProps;
    private static KafkaProducer<Integer, Customer> producer;
    private static ProducerRecord<String, String> record;
    private static final String shemaUrl = null;


    public static void main(String[] args) {

        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("schema.registry.url", shemaUrl);
        producer = new KafkaProducer<Integer, Customer>(kafkaProps);
        record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "France");
        sendMessage();


    }

    private static void sendMessage() {
        while (true) {
            Customer customer = CustomerGenerator.getNext();
            System.out.println(customer.toString());
            String topic="CustomerCountry";
            ProducerRecord<Integer, Customer> record = new ProducerRecord<>(topic, customer.getCustomerId(), customer);
            producer.send(record);

        }
    }
}
