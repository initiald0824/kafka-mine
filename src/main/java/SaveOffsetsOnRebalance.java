import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * 再平衡监听器
 */
public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
    private static KafkaConsumer<String, String> consumer;

    public SaveOffsetsOnRebalance(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        Collection<String> topics= Collections.singletonList("topics");
        consumer.subscribe(topics, new SaveOffsetsOnRebalance(consumer));
        consumer.poll(0);
        for (TopicPartition partition : consumer.assignment()) {
            consumer.seek(partition,getOffsetFromDB(partition));
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
                storeRecordInDB(record);
                storeOffsetInDB(record.topic(), record.partition(),record.offset());
            }
            commitDBTransaction();
        }
    }

    private static void storeOffsetInDB(String topic, int partition, long offset) {

    }

    private static void storeRecordInDB(ConsumerRecord<String, String> record) {

    }

    private static void processRecord(ConsumerRecord<String, String> record) {

    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        commitDBTransaction();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        for (TopicPartition partition : collection) {
            consumer.seek(partition, getOffsetFromDB(partition));
        }
    }

    private static void commitDBTransaction() {

    }

    private static long getOffsetFromDB(TopicPartition partition) {
        return 0;
    }
}
