/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class PartitionCounter {

    protected static final String MAX_PARTITONS = "max.partitions";
    private static final String CONSUMER_OFFSETS = "__consumer_offsets";
    private static final String INTERNAL_PARTITION_PREFIX = "__redhat_";

    public static long countExistingPartitions(Admin admin) throws InterruptedException, ExecutionException {
        List<String> topicNames = admin.listTopics()
                .listings()
                .get()
                .stream()
                .map(TopicListing::name)
                .filter(name -> !name.startsWith(INTERNAL_PARTITION_PREFIX) && !CONSUMER_OFFSETS.equals(name))
                .collect(Collectors.toList());

        return admin.describeTopics(topicNames)
                .all()
                .get()
                .values()
                .stream()
                .map(description -> (long) description.partitions().size())
                .reduce(0L, Long::sum);
    }

    public static int getMaxPartitions(Map<String, ?> configs) {
        int maxPartitions;
        try {
            maxPartitions = Optional.ofNullable(configs.get(MAX_PARTITONS))
                    .map(Object::toString)
                    .map(Integer::valueOf)
                    .orElse(-1);
        } catch (NumberFormatException e) {
            maxPartitions = -1;
        }
        return maxPartitions;
    }

}
