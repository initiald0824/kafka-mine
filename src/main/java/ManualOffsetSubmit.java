import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ManualOffsetSubmit {

    private static HashMap<TopicPartition, OffsetAndMetadata> currentOffsets;
    private static KafkaConsumer<String, String> consumer;
    private static Set<String> topics;

    public static void main(String[] args) {
        Properties props = new Properties();
        consumer = new KafkaConsumer<String, String>(props);
        currentOffsets = new HashMap<>();
        topics = Collections.singleton("topics");
        int count=0;

        mapSubmit(consumer, currentOffsets, count);
        reBalanceListener();

    }

    private static void reBalanceListener() {
        try {
            consumer.subscribe(topics,new HandleRebalance());
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record);
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));

                }
                consumer.commitAsync(currentOffsets,null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }


    /**
     * 偏移量保存在一个map中
     * @param consumer
     * @param currentOffsets
     * @param count
     */
    private static void mapSubmit(KafkaConsumer<String, String> consumer, HashMap<TopicPartition, OffsetAndMetadata> currentOffsets, int count) {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s,%s,%s,%s,%s", record.topic(), record.offset(), record.partition(), record.key());
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                if (count % 1000 == 0) {
                    consumer.commitAsync(currentOffsets,null);
                }
                count++;
            }
        }
    }

    private static class HandleRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println(currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        }
    }

}
