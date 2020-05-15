package com.kafka.training.session1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class MyProducerPerformance {
    private Logger logger = LoggerFactory.getLogger(MyProducerCallable.class);
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        //read the arguments
        new MyProducerPerformance().executePerformanceTest();
    }

    private void executePerformanceTest() throws InterruptedException, ExecutionException {
        String topic = System.getProperty("topic");
        int numberOfThreads = Integer.parseInt(System.getProperty("num-threads"));
        int numberOfMessages = Integer.parseInt(System.getProperty("messages"));
        String testId = System.getProperty("test-id");

        //Create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create producer
        ExecutorService threadPool = Executors.newFixedThreadPool(numberOfThreads);
        ExecutorCompletionService<List<String>> completionService = new ExecutorCompletionService<List<String>>(threadPool);

        int messagesPerThread = numberOfMessages / numberOfThreads;
        int extraMessages = 0;
        if (numberOfMessages % numberOfThreads != 0) {
            extraMessages = numberOfMessages % numberOfThreads;
        }

        for (int i = 0; i < numberOfThreads; i++) {
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            int messageCount = messagesPerThread;
            if (i != 0) {
                messageCount = messageCount + extraMessages;
            }
            completionService.submit(new MyProducerCallable(topic, "Hello Test", producer, messageCount, testId));
        }

        List<String> reportData = new ArrayList<String>();
        for (int i = 0; i < numberOfThreads; i++) {
            Future<List<String>> producerOutput = completionService.take();
            List<String> data = producerOutput.get();
            logger.info(data.toString());
            reportData.addAll(data);
        }
        logger.info("Number of messages sent - " + reportData.size());

        if (completionService.poll() == null) {
            threadPool.shutdown();
        }


//        System.exit(0);
        //create threads based on the argument
        //send messages
        //create report using callback method
        //
    }
}

//Create Callable which returns the list of comma separated string
//we can use completion service to wait for completing all the threads and then write to CSV file

class MyProducerCallable implements Callable<List<String>> {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final String message;
    private final int numberOfMessages;
    private final String testId;
    private Logger logger = LoggerFactory.getLogger(MyProducerCallable.class);


    MyProducerCallable(String topic, String message, KafkaProducer<String, String> producer, int noOfMessages, String testId) {
        this.topic = topic;
        this.message = message;
        this.producer = producer;
        this.numberOfMessages = noOfMessages;
        this.testId = testId;
    }

    public List<String> call() throws Exception {
        final List<String> producerOutput = new ArrayList<String>();
        String key = "id_";
        for (int i = 0; i < numberOfMessages; i++) {
            key = key + Integer.toString(i);
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

            //send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                        StringBuilder outputBuilder = new StringBuilder();
                        outputBuilder.append(testId).append(",").append(recordMetadata.partition()).append(",").append(recordMetadata.offset())
                                .append(",").append(recordMetadata.timestamp());
                        producerOutput.add(outputBuilder.toString());
                    } else {
                        logger.error("Error while producing message", e);
                    }
                }
            });
        }
        // flush data
        producer.flush();
        // close data
        producer.close();
        return producerOutput;
    }
}
