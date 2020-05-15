package com.kafka.training.session2;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ConsumerExactlyOnce {
    private DatabaseClient databaseClient;

    Logger logger = LoggerFactory.getLogger(ConsumerExactlyOnce.class);
    KafkaConsumer<String, String> consumer = null;
    String bootstrapServer = "localhost:9092";
    String topic = "second_topic";
    String groupId = "my-sixth-application";

    public ConsumerExactlyOnce() {
        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Create the consumer
        consumer = new KafkaConsumer<String, String>(properties);
        this.databaseClient = new DatabaseClient();
    }

    public static void main(String[] args) throws SQLException {
        new ConsumerExactlyOnce().execute();
    }

    private void execute() throws SQLException {
        Collection<String> topics = Arrays.asList(topic);
        consumer.subscribe(topics, new MyRebalanceListener(consumer, databaseClient));
        consumer.poll(0);

        for (TopicPartition partition: consumer.assignment())
            consumer.seek(partition, databaseClient.getOffsetForPartition(partition.partition(), topic));

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
                databaseClient.persistRecordData(record.key(), record.value());
                databaseClient.persistPartitionOffset(record.partition(), record.offset(),
                        record.topic());
            }
            databaseClient.commitTransaction();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        logger.info("Record processed - " + record.key() + "->" + record.value());
    }

}

class MyRebalanceListener implements ConsumerRebalanceListener {
    KafkaConsumer<String, String> consumer = null;
    private DatabaseClient databaseClient;
    Logger logger = LoggerFactory.getLogger(MyRebalanceListener.class);
    public MyRebalanceListener(KafkaConsumer<String, String> consumer, DatabaseClient databaseClient) {
        this.consumer = consumer;
        this.databaseClient = databaseClient;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Listener Called on partition revoked");
        databaseClient.commitTransaction();
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Listener called on partition assignment");
        for(TopicPartition partition: partitions) {
            try {
                consumer.seek(partition, databaseClient.getOffsetForPartition(partition.partition(), partition.topic()));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}


