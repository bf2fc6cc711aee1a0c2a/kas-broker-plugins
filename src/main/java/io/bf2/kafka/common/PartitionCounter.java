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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PartitionCounter implements Closeable {

    public static final long DEFAULT_SCHEDULE_PERIOD_MILLIS = 5_000;

    protected static final String MAX_PARTITONS = "max.partitions";
    private static final String CONSUMER_OFFSETS = "__consumer_offsets";
    private static final String INTERNAL_PARTITION_PREFIX = "__redhat_";

    private static final ConfigDef configDef = new ConfigDef()
            .define(MAX_PARTITONS, ConfigDef.Type.INT, -1, ConfigDef.Importance.MEDIUM, "Max partitions");

    private Timer timer;
    private final Admin admin;

    public AtomicLong existingPartitionCount;

    public PartitionCounter(Map<String, ?> config) {
        existingPartitionCount = new AtomicLong(0);
        this.admin = LocalAdminClient.create(config);
        this.timer = new Timer("PartitionCounter-Admin-" + Thread.currentThread().getId(), true);
    }

    @Override
    public void close() {
        if (timer != null) {
            timer.cancel();
        }

        if (admin != null) {
            admin.close(Duration.ofMillis(500));
        }
    }

    public void startPeriodicCounter(long period) {
        TimerTask task = new TimerTask() {

            @Override
            public void run() {
                try {
                    existingPartitionCount.set(countExistingPartitions());
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(task, new Date(), 10000);
    }

    public long countExistingPartitions() throws InterruptedException, ExecutionException, TimeoutException {
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
                .map(description -> (long) description.partitions().size())
                .reduce(0L, Long::sum);
    }

    public static int getMaxPartitions(Map<String, ?> configs) {
        int maxPartitions = -1;
        try {
            maxPartitions = new AbstractConfig(configDef, configs).getInt(MAX_PARTITONS);
        } catch (ConfigException | NullPointerException | NumberFormatException e) {
            maxPartitions = -1;
        }
        return maxPartitions;
    }

}
