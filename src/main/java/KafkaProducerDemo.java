import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 生产者的配置和提交（简单提交，同步提交和异步提交）
 */
public class KafkaProducerDemo {


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "France");


        simpleSendMessage(producer, record);
        synSendMessage(producer, record);
        asynSendMessage(producer, record);

    }

    /**
     * 异步提交消息
     * @param producer
     * @param record
     */
    private static void asynSendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record, new DemoProducerCallback());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步提交消息
     * @param producer
     * @param record
     */
    private static void synSendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 普通发送消息
     * @param producer
     * @param record
     */
    private static void simpleSendMessage(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class DemoProducerCallback implements Callback {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }
}
