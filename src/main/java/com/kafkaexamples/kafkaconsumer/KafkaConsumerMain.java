package com.kafkaexamples.kafkaconsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaConsumerMain {
    private static final Logger log = Logger.getAnonymousLogger();

    public static void main(String[] args) {
        final int consumersCount = consumersCount(args);
        final String bootstrapServer = bootstrapServer(args);
        final Collection<String> topics = subscribedTopics(args);

        ConsumersGroup<String, String> group = new ConsumersGroup<>("org.apache.kafka.common.serialization.StringDeserializer");

        for (int i = 0; i <consumersCount; i++) {
            group.spinNewConsumer(bootstrapServer, topics, KafkaConsumerMain::printRecordInfo);
        }

        waitForExitInput();

        group.shutdownAll();
    }

    private static int consumersCount(String[] args) {
        String countValue = args.length > 1 ? args[1] : "1";
        try {
            return Integer.parseInt(countValue);
        } catch (NumberFormatException ignored) {
            return 1;
        }
    }

    private static String bootstrapServer(String[] args) {
        return args.length > 0 ? args[0] : "localhost:9094";
    }

    private static Collection<String> subscribedTopics(String[] args) {
        if (args.length <= 2) {
            return Collections.emptyList();
        }
        Collection<String> topics = Arrays.stream(args).skip(2).toList();
        log.info("Found " + topics.size() + " topic subscription(s)");

        return topics;
    }

    private static void printRecordInfo(ConsumerRecord<String, String> r) {
        String tpl = "topic = %s, partition = %s, offset = %s, key = %s, value = %s";
        log.info(String.format(tpl, r.topic(), r.partition(), r.offset(), r.key(), r.value()));
    }

    private static void waitForExitInput() {
        Scanner scanner = new Scanner(System.in);
        String cmd;
        do {
            cmd = scanner.nextLine();
        } while (!"/exit".equalsIgnoreCase(cmd));
    }
}
