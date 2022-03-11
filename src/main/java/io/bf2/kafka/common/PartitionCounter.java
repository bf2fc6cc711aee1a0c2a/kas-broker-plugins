/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bf2.kafka.authorizer.AclLoggingConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartitionCounter implements AutoCloseable {

    public static final String MAX_PARTITIONS = "max.partitions";
    public static final String TIMEOUT_SECONDS =
            "strimzi.authorization.custom-authorizer.partition-counter.timeout-seconds";
    public static final String PRIVATE_TOPIC_PREFIX = "strimzi.authorization.custom-authorizer.partition-counter.private-topic-prefix";
    public static final String SCHEDULE_INTERVAL_SECONDS = "strimzi.authorization.custom-authorizer.partition-counter.schedule-interval-seconds";

    static final int DEFAULT_MAX_PARTITIONS = -1;
    static final int DEFAULT_TIMEOUT_SECONDS = 10;
    static final int DEFAULT_SCHEDULE_INTERVAL_SECONDS = 15;
    static final String DEFAULT_PRIVATE_TOPIC_PREFIX = "__redhat_";

    private static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
    private static final String TRANSACTION_STATE_TOPIC_NAME = "__transaction_state";

    private static final ConfigDef configDef = new ConfigDef()
            .define(MAX_PARTITIONS, ConfigDef.Type.INT, DEFAULT_MAX_PARTITIONS, ConfigDef.Importance.MEDIUM, "Max partitions")
            .define(PRIVATE_TOPIC_PREFIX, ConfigDef.Type.STRING, DEFAULT_PRIVATE_TOPIC_PREFIX, ConfigDef.Importance.MEDIUM, "Internal Partition Prefix")
            .define(TIMEOUT_SECONDS, ConfigDef.Type.INT, DEFAULT_TIMEOUT_SECONDS,ConfigDef.Importance.MEDIUM, "Timeout duration for listing and describing topics")
            .define(SCHEDULE_INTERVAL_SECONDS, ConfigDef.Type.INT, DEFAULT_SCHEDULE_INTERVAL_SECONDS,ConfigDef.Importance.MEDIUM, "Schedule interval for scheduled counter");

    private static final Logger log = LoggerFactory.getLogger(AclLoggingConfig.class);

    private static PartitionCounter partitionCounter;

    private final int maxPartitions;

    private final Admin admin;
    private AtomicInteger handles;
    private AtomicInteger existingPartitionCount;
    private AtomicInteger remainingPartitionBudget;

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledPartitionCounter;

    private final Integer requestTimeout;
    private final String privateTopicPrefix;
    private final Integer scheduleIntervalSeconds;

    public static synchronized PartitionCounter create(Map<String, ?> config) {
        if (partitionCounter == null) {
            partitionCounter = new PartitionCounter(config);
        }
        partitionCounter.start();
        partitionCounter.handles.incrementAndGet();
        return partitionCounter;
    }

    PartitionCounter(Map<String, ?> config) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, config);

        handles = new AtomicInteger(0);
        admin = LocalAdminClient.create(config);
        existingPartitionCount = new AtomicInteger(0);
        remainingPartitionBudget = new AtomicInteger(0);

        requestTimeout = parsedConfig.getInt(TIMEOUT_SECONDS);
        maxPartitions = getMaxPartitionsFromConfig(parsedConfig);
        privateTopicPrefix = parsedConfig.getString(PRIVATE_TOPIC_PREFIX);
        scheduleIntervalSeconds = parsedConfig.getInt(SCHEDULE_INTERVAL_SECONDS);

        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("partition-counter").setDaemon(true).build();
        scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    @Override
    public void close() {
        if (handles.decrementAndGet() == 0) {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }

            if (admin != null) {
                admin.close(Duration.ofMillis(500));
            }
        }
    }

    public int getExistingPartitionCount() {
        return existingPartitionCount.get();
    }

    public int getRemainingPartitionBudget() {
        return remainingPartitionBudget.get();
    }

    public enum ReservationResponse {
        SUCCEEDED, FAILED, REJECTED
    }

    public ReservationResponse reservePartitions(int numPartitions) {
        int attempts = 0;
        while (attempts++ < 3) {
            int budget = partitionCounter.getRemainingPartitionBudget();
            int proposedNewBudget = budget - numPartitions;
            if (proposedNewBudget < 0) {
                return ReservationResponse.REJECTED;
            }
            if (remainingPartitionBudget.compareAndSet(budget, proposedNewBudget)) {
                return ReservationResponse.SUCCEEDED;
            }
        }

        log.warn("Failed to safely reserve requested partitions in cache."
                + " There may be too many CreateTopic requests in flight.");
        return ReservationResponse.FAILED;
    }

    public int getMaxPartitions() {
        return maxPartitions;
    }

    private static int getMaxPartitionsFromConfig(AbstractConfig config2) {
        try {
            return config2.getInt(MAX_PARTITIONS);
        } catch (ConfigException | NullPointerException | NumberFormatException e) {
            log.warn("An invalid or abset value was provided for " + MAX_PARTITIONS + "in the broker configs."
                    + " A value of -1 will be used to indicate that no max will be enforced.");
            return -1;
        }
    }

    private void start() {
        if (scheduledPartitionCounter == null) {
            scheduledPartitionCounter = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    int existingPartitions = countExistingPartitions();
                    existingPartitionCount.set(existingPartitions);
                    remainingPartitionBudget.set(maxPartitions - existingPartitions);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Exception occurred when counting partitions", e);
                }
            }, 0, scheduleIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    public int countExistingPartitions() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = admin.listTopics()
                .listings()
                .get(requestTimeout, TimeUnit.SECONDS)
                .stream()
                .map(TopicListing::name)
                .filter(name -> !name.startsWith(privateTopicPrefix)
                        && !GROUP_METADATA_TOPIC_NAME.equals(name) && !TRANSACTION_STATE_TOPIC_NAME.equals(name))
                .collect(Collectors.toList());

        return admin.describeTopics(topicNames)
                .all()
                .get(requestTimeout, TimeUnit.SECONDS)
                .values()
                .stream()
                .map(description -> description.partitions().size())
                .reduce(0, Integer::sum);
    }
}
