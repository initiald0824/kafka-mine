import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

/**
 * 自动提交偏移量
 */
public class AutoOffsetSubmit {
    public static void main(String[] args) {
        Properties props = new Properties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        synSubmit(consumer);
        aSynSubmit(consumer);
        combineSubmit(consumer);
    }

    /**
     * 同步提交和异步提交的组合使用
     * @param consumer
     */
    private static void combineSubmit(KafkaConsumer<String, String> consumer) {
        try {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s,%s,%s,%s,%s", record.topic(), record.offset(), record.partition(), record.key());

            }
            consumer.commitAsync();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 同步提交偏移量
     * @param consumer
     */
    private static void synSubmit(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s,%s,%s,%s", record.topic(), record.partition(), record.offset(), record.key(), record.value());

            }
            try {
                consumer.commitSync();//同步提交
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 异步提交偏移量
     * @param consumer
     */
    private static void aSynSubmit(KafkaConsumer<String, String> consumer) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s,%s,%s,%s,%s", record.topic(), record.partition(), record.offset(), record.key(), record.value());

            }
//            consumer.commitAsync();
            consumer.commitAsync(new OffsetCommitCallback() {

                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e == null) {
                        System.out.println("commit failed");
                    }
                }
            });
        }
    }
}
