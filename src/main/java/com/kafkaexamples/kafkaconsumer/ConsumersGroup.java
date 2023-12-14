package com.kafkaexamples.kafkaconsumer;

import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.WakeupException;

public final class ConsumersGroup<K, V> {
    private static final Logger log = Logger.getAnonymousLogger();

    private final String groupId = ConsumersGroup.class.getName() + "-" + UUID.randomUUID();
    private final ExecutorService executor;
    private final Map<UUID, Consumer<K, V>> consumers;

    private final String keyDeserializerClass;
    private final String valueDeserializerClass;
    private final Duration timeout;

    public ConsumersGroup(String deserializerClassConfig) {
        this(deserializerClassConfig, deserializerClassConfig);
    }

    public ConsumersGroup(String keyDeserializerClassConfig, String valueDeserializerClassConfig) {
        this(valueDeserializerClassConfig, keyDeserializerClassConfig, Duration.ofSeconds(5));
    }

    public ConsumersGroup(String keyDeserializerClassConfig, String valueDeserializerClassConfig,
                          Duration timeoutConfig) {
        keyDeserializerClass = keyDeserializerClassConfig;
        valueDeserializerClass = valueDeserializerClassConfig;
        timeout = timeoutConfig;

        executor = Executors.newCachedThreadPool();
        consumers = new ConcurrentHashMap<>();
    }

    public UUID spinNewConsumer(String bootstrapServer, Collection<String> topics,
                                java.util.function.Consumer<ConsumerRecord<K, V>> recordClientConsumer) {
        UUID uuid = newUuid();
        Consumer<K, V> newConsumer = addNewConsumer(bootstrapServer, keyDeserializerClass, valueDeserializerClass, uuid);

        executor.submit(() -> startConsumer(newConsumer, uuid, topics, recordClientConsumer));
        registerShutDownHook(newConsumer);

        return uuid;
    }

    private UUID newUuid() {
        UUID uuid = UUID.randomUUID();

        synchronized (this) {
            while (consumers.containsKey(uuid)) {
                uuid = UUID.randomUUID();
            }
        }
        return uuid;
    }

    private Consumer<K, V> addNewConsumer(String bootstrapServer, String keyDeserializerClass,
                                          String valueDeserializerClass, UUID consumerUuid) {
        KafkaConsumer<K, V> newConsumer = new KafkaConsumer<>(properties(bootstrapServer, keyDeserializerClass,
                                                                         valueDeserializerClass, consumerUuid));
        consumers.put(consumerUuid, newConsumer);

        return newConsumer;
    }

    private Properties properties(String bootstrapServer, String keyDeserializerClass, String valueDeserializerClass,
                                  UUID consumerUuid) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        props.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");

        props.put(GROUP_ID_CONFIG, groupId);
        props.put(CLIENT_ID_CONFIG, groupId + "-" + consumerUuid);

        return props;
    }

    private void startConsumer(Consumer<K, V> newConsumer, UUID consumerUuid, Collection<String> topics,
                               java.util.function.Consumer<ConsumerRecord<K, V>> recordClientConsumer) {
        printTopicPartitionsInfo(newConsumer, topics);
        log.info("Subscribing consumer with UUID " + consumerUuid + " to topic " + topics + "...");

        try {
            newConsumer.subscribe(topics);

            while (true) { // poll indefinitely, until the consumer is shutdown
                for (ConsumerRecord<K, V> record : newConsumer.poll(timeout)) {
                    recordClientConsumer.accept(record);
                }
                newConsumer.commitAsync();
            }
        } catch (WakeupException ignored) {
            log.info("Consumer woke up");
        } finally {
            commitSyncAndClose(newConsumer);
        }
    }

    private void printTopicPartitionsInfo(Consumer<K, V> consumer, Collection<String> topics) {
        topics.forEach(topic -> printTopicPartitionsInfo(consumer, topic));
    }

    private void printTopicPartitionsInfo(Consumer<K, V> consumer, String topic) {
        log.info("Getting partitions info for topic " + topic + "...");
        List<PartitionInfo> info;
        try {
            info = consumer.partitionsFor(topic, timeout);
        } catch (KafkaException e) {
            log.severe("Error getting partitions info " + e.getMessage());
            return;
        }

        log.info(info.size() + " partitions");
        String msg = info.stream().map(PartitionInfo::partition).map(String::valueOf).collect(joining(", "));

        log.info("Partition ids: " + msg);
    }

    private void commitSyncAndClose(Consumer<K, V> consumer) {
        try {
            log.info("Committing offset synchronously...");
            consumer.commitSync();
        } catch (CommitFailedException e) {
            // commitSync retries committing on its own as long as there is no error that can’t be recovered.
            // If this happens, there is not much we can do except log an error.
            log.severe("Commit failed: " + e.getMessage());
        } finally {
            log.info("Closing...");
            consumer.close();
        }
    }

    private static <K, V> void registerShutDownHook(Consumer<K, V> newConsumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(newConsumer::wakeup));
    }

    public void shutdownAll() {
        consumers.values().forEach(Consumer::wakeup);
    }
}
