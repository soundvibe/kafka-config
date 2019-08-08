package net.soundvibe.kafka.config.streams;

import org.apache.kafka.common.*;
import org.apache.kafka.streams.processor.*;

import java.util.*;

public class TestPartitionGrouper implements PartitionGrouper {
    @Override
    public Map<TaskId, Set<TopicPartition>> partitionGroups(Map<Integer, Set<String>> topicGroups, Cluster metadata) {
        return new HashMap<>();
    }
}
