package io.bf2.kafka.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

/**
 * A PartitionCounter counts partitions. It is intended to be used as a shared instance, which
 * schedules partition counting at regular intervals in the background, and exposes a remaining
 * budget of partitions that can be reserved. The budget is the difference between the max
 * partitions (as specified by the {@link Config#MAX_PARTITIONS} broker property) and the current count of
 * existing partitions.
 *
 * Expected usage is as follows:
 * <ol>
 * <li>Get a handle to the shared instances, using the static {@link #create(Map<String, ?>)} method</li>
 * <li>Reserve partitions using {@link #reservePartitions(int)}</li>
 * <li>If it returns true, then the request was within the budget, and the partitions can be
 * created.</li>
 * <li>If instead it returns false, the request may have resulted in more partitions being created
 * than the partition limit. In this case, {@link #countExistingPartitions()} may optionally be used
 * to try to get a more accurate partition count.</li>
 * </ol>
 */
public class PartitionCounter implements AutoCloseable {
    private static final String GROUP_METADATA_TOPIC_NAME = "__consumer_offsets";
    private static final String TRANSACTION_STATE_TOPIC_NAME = "__transaction_state";

    private static final Logger log = LoggerFactory.getLogger(PartitionCounter.class);

    private static volatile PartitionCounter partitionCounter;
    private static final AtomicInteger handles = new AtomicInteger();

    private final int maxPartitions;

    private final Admin admin;
    private final AtomicInteger existingPartitionCount;
    private final AtomicInteger remainingPartitionBudget;

    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledPartitionCounter;

    private final Integer requestTimeout;
    private final String privateTopicPrefix;
    private final Integer scheduleIntervalSeconds;
    private final boolean limitEnforced;

    /**
     * Creates the shared PartitionCounter if it doesn't already exist. Returns the existing one if it
     * was created already.
     *
     * @param config the map of Kafka broker properties.
     * @return the shared PartitionCounter.
     */
    public static synchronized PartitionCounter create(Map<String, ?> config) {
        if (partitionCounter == null) {
            partitionCounter = new PartitionCounter(config);
            partitionCounter.start();
        }
        handles.incrementAndGet();
        return partitionCounter;
    }

    private static synchronized void release() {
        if (handles.updateAndGet(i -> i > 0 ? i - 1 : i) == 0 && partitionCounter != null) {
            try {
                if (partitionCounter.scheduler != null) {
                    partitionCounter.scheduler.shutdownNow();
                }

                if (partitionCounter.admin != null) {
                    partitionCounter.admin.close(Duration.ofMillis(500));
                }

            } finally {
                partitionCounter = null;
            }
        }
    }

    static synchronized int getHandleCount() {
        return handles.get();
    }

    PartitionCounter(Map<String, ?> config) {
        AbstractConfig parsedConfig = new AbstractConfig(Config.PARTITION_COUNTER_CONFIG_DEF, config);

        admin = LocalAdminClient.create(config);
        existingPartitionCount = new AtomicInteger(0);
        remainingPartitionBudget = new AtomicInteger(0);

        requestTimeout = parsedConfig.getInt(Config.TIMEOUT_SECONDS);
        maxPartitions = getMaxPartitionsFromConfig(parsedConfig);
        privateTopicPrefix = parsedConfig.getString(Config.PRIVATE_TOPIC_PREFIX);
        scheduleIntervalSeconds = parsedConfig.getInt(Config.SCHEDULE_INTERVAL_SECONDS);
        limitEnforced = parsedConfig.getBoolean(Config.LIMIT_ENFORCED);

        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("partition-counter").setDaemon(true).build();
        scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    @Override
    public void close() {
        release();
    }

    /**
     * @return the existing user partition count as of the last check, or 0 if no check has occurred
     *         yet.
     */
    public int getExistingPartitionCount() {
        return existingPartitionCount.get();
    }

    /**
     * @return the remaining partition budget, which is calculated as the existing user partition count
     *         as of the last check, minus any reserved partitions since that time.
     */
    public int getRemainingPartitionBudget() {
        return remainingPartitionBudget.get();
    }

    /**
     * @param numPartitions the number of partitions to reserve from the remaining budget.
     * @return false if the reservation request has exceeded the budget, otherwise true.
     */
    public boolean reservePartitions(int numPartitions) {
        return remainingPartitionBudget.updateAndGet(budget -> budget - numPartitions) >= 0;
    }

    /**
     * @return the value of the {@link Config#MAX_PARTITIONS} key in the broker configs, or a default of
     *         {@link Config#DEFAULT_MAX_PARTITIONS} if not set.
     */
    public int getMaxPartitions() {
        return maxPartitions;
    }

    /**
     * @return the value of the {@link Config#PRIVATE_TOPIC_PREFIX} key in the broker configs, or a default of
     *         {@link Config#DEFAULT_NO_PRIVATE_TOPIC_PREFIX} if not set.
     */
    public String getPrivateTopicPrefix() {
        return privateTopicPrefix;
    }

    /**
     * @return true if the {@link Config#LIMIT_ENFORCED} property is explicitly set to true, else false.
     */
    public boolean isLimitEnforced() {
        return limitEnforced;
    }

    /**
     * Counts the number of user partitions in the cluster. It is used internally by this class,
     * scheduled at regular intervals to get a value for the existing number of partitions. However, it
     * can also be used synchronously to get a value at any time.
     *
     * Prefer the default flow of using {@link #reservePartitions(int)} and
     * {@link @getRemainingBudget()} by default, and then this method can then be used as a fallback to
     * re-validate if reservePartitions() returns false.
     *
     * @return the number of user partitions that currently exist in the cluster
     * @throws InterruptedException if the current thread was interrupted while listing or describing
     *                              topics.
     * @throws ExecutionException   if the computation threw an exception, while either listing or
     *                              describing topics.
     * @throws TimeoutException     if the list or describe topic operations timed out. The timeout
     *                              duration used for each is the number of seconds specified in the
     *                              broker property specified by the value of {@link Config#TIMEOUT_SECONDS},
     *                              falling back to a default of {@link Config#DEFAULT_TIMEOUT_SECONDS} if not
     *                              set.
     */
    public int countExistingPartitions() throws InterruptedException, ExecutionException, TimeoutException {
        List<String> topicNames = admin.listTopics()
                .listings()
                .get(requestTimeout, TimeUnit.SECONDS)
                .stream()
                .map(TopicListing::name)
                .filter(name -> !isInternalTopic(name))
                .collect(Collectors.toList());

        return admin.describeTopics(topicNames)
                .all()
                .get(requestTimeout, TimeUnit.SECONDS)
                .values()
                .stream()
                .map(description -> description.partitions().size())
                .reduce(0, Integer::sum);
    }

    public boolean isInternalTopic(String name) {
        return isInternalTopic(name, privateTopicPrefix);
    }

    public static boolean isInternalTopic(String name, String privateTopicPrefix) {
        return GROUP_METADATA_TOPIC_NAME.equals(name)
                || TRANSACTION_STATE_TOPIC_NAME.equals(name)
                || (!privateTopicPrefix.isBlank() && name.startsWith(privateTopicPrefix));
    }

    private static int getMaxPartitionsFromConfig(AbstractConfig config) {
        try {
            return config.getInt(Config.MAX_PARTITIONS);
        } catch (ConfigException | NullPointerException | NumberFormatException e) {
            log.warn("An invalid or absent value was provided for " + Config.MAX_PARTITIONS + " in the broker configs."
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
}
