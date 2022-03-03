/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartitionCounter implements Closeable {

    public static final long SCHEDULE_PERIOD_MILLIS = 10_000;
    public static final String MAX_PARTITIONS = "max.partitions";

    private static final String CONSUMER_OFFSETS = "__consumer_offsets";
    private static final String INTERNAL_PARTITION_PREFIX = "__redhat_";

    private static final ConfigDef configDef = new ConfigDef()
            .define(MAX_PARTITIONS, ConfigDef.Type.INT, -1, ConfigDef.Importance.MEDIUM, "Max partitions");

    private static PartitionCounter partitionCounter;

    private final int maxPartitions;

    private final Admin admin;

    private AtomicInteger existingPartitionCount;

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledPartitionCounter;

    public static synchronized PartitionCounter create(Map<String, ?> config) {
        if (partitionCounter == null) {
            partitionCounter = new PartitionCounter(config);
        }
        return partitionCounter;
    }

    PartitionCounter(Map<String, ?> config) {
        this.admin = LocalAdminClient.create(config);
        existingPartitionCount = new AtomicInteger(0);
        scheduler = Executors.newScheduledThreadPool(1);
        maxPartitions = setMaxPartitions(config);
        startPeriodicCounter();
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        if (admin != null) {
            admin.close(Duration.ofMillis(500));
        }
    }

    /**
     * @return the existingPartitionCount
     */
    public int getExistingPartitionCount() {
        return existingPartitionCount.get();
    }

    public int getMaxPartitions() {
        return maxPartitions;
    }

    private int setMaxPartitions(Map<String, ?> configs) {
        try {
            return new AbstractConfig(configDef, configs).getInt(MAX_PARTITIONS);
        } catch (ConfigException | NullPointerException | NumberFormatException e) {
            return -1;
        }
    }

    private void startPeriodicCounter() {
        if (scheduledPartitionCounter == null) {
            scheduledPartitionCounter = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    int existingPartitions = countExistingPartitions();
                    existingPartitionCount.set(existingPartitions);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            }, 0, SCHEDULE_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private int countExistingPartitions() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = admin.listTopics()
                .listings()
                .get(10, TimeUnit.SECONDS)
                .stream()
                .map(TopicListing::name)
                .filter(name -> !name.startsWith(INTERNAL_PARTITION_PREFIX) && !CONSUMER_OFFSETS.equals(name))
                .collect(Collectors.toList());

        return admin.describeTopics(topicNames)
                .all()
                .get(10, TimeUnit.SECONDS)
                .values()
                .stream()
                .map(description -> description.partitions().size())
                .reduce(0, Integer::sum);
    }
}
