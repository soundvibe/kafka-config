package net.soundvibe.kafka.config.streams;

import net.soundvibe.kafka.config.AbstractConfigBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.RocksDBConfigSetter;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

public final class StreamsConfigBuilder extends AbstractConfigBuilder<StreamsConfigBuilder> {

    private StreamsConfigBuilder() {}

    public static StreamsConfigBuilder create() {
        return new StreamsConfigBuilder();
    }

    /**
     * An identifier for the stream processing application. Must be unique within the Kafka cluster.
     * It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
     */
    public StreamsConfigBuilder withApplicationId(String applicationId) {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        return this;
    }

    /**
     * Version of the built-in metrics to use.
     */
    public StreamsConfigBuilder withBuiltinMetricsVersion(String builtinMetricsVersion) {
        props.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtinMetricsVersion);
        return this;
    }

    /**
     * The replication factor for change log topics and repartition topics created by the stream processing application.
     */
    public StreamsConfigBuilder withReplicationFactor(int replicationFactor) {
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
        return this;
    }

    /**
     * The replication factor for change log topics and repartition topics created by the stream processing application.
     */
    public StreamsConfigBuilder withStateDir(Path stateDir) {
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return this;
    }

    /**
     * Maximum number of memory bytes to be used for statestore cache across all threads.
     */
    public StreamsConfigBuilder withStateStoreCacheMaxBytes(long stateStoreCacheMaxBytes) {
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, stateStoreCacheMaxBytes);
        return this;
    }

    /**
     * Maximum number of memory bytes to be used for buffering across all threads
     */
    public StreamsConfigBuilder withCacheMaxBytesBuffering(long cacheMaxBytesBuffering) {
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytesBuffering);
        return this;
    }

    /**
     * An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer,
     * with pattern '<client.id>-StreamThread-<threadSequenceNumber>-<consumer|producer|restore-consumer>'
     */
    @Override
    public StreamsConfigBuilder withClientId(String clientId) {
        return super.withClientId(clientId);
    }

    /**
     * The frequency in milliseconds with which to delete fully consumed records from repartition topics.
     * Purging will occur after at least this value since the last purge, but may be delayed until later.
     * (Note, unlike <code>commit.interval.ms</code>, the default for this value remains unchanged when <code>processing.guarantee</code> is set to <code>EXACTLY_ONCE_V2</code>).
     */
    public StreamsConfigBuilder withRepartitionPurgeInterval(Duration repartitionPurgeInterval) {
        props.put(StreamsConfig.REPARTITION_PURGE_INTERVAL_MS_CONFIG, repartitionPurgeInterval.toMillis());
        return this;
    }

    /**
     * Exception handling class that implements the <code>org.apache.kafka.streams.errors.DeserializationExceptionHandler</code> interface.
     */
    public StreamsConfigBuilder withDefaultDeserializationExceptionHandler(Class<? extends DeserializationExceptionHandler> defaultDeserializationExceptionHandler) {
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, defaultDeserializationExceptionHandler);
        return this;
    }

    /**
     * Default serializer / deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface.
     * Note when windowed serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via
     * DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS or DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS as well
     */
    public StreamsConfigBuilder withDefaultKeySerde(Class<? extends Serde<?>> defaultKeySerde) {
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, defaultKeySerde);
        return this;
    }

    /**
     * Default serializer / deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface.
     * Note when windowed serde class is used, one needs to set the inner serde class that implements the <code>org.apache.kafka.common.serialization.Serde</code> interface via
     * DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS or DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS as well
     */
    public StreamsConfigBuilder withDefaultValueSerde(Class<? extends Serde<?>> defaultValueSerde) {
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, defaultValueSerde);
        return this;
    }

    /**
     * Exception handling class that implements the <code>org.apache.kafka.streams.errors.ProductionExceptionHandler</code> interface.
     */
    public StreamsConfigBuilder withDefaultProductionExceptionHandler(Class<? extends ProductionExceptionHandler> defaultProductionExceptionHandler) {
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, defaultProductionExceptionHandler);
        return this;
    }

    /**
     * Default timestamp extractor class that implements the <code>org.apache.kafka.streams.processor.TimestampExtractor</code> interface.
     */
    public StreamsConfigBuilder withDefaultTimestampExtractor(Class<? extends TimestampExtractor> defaultTimestampExtractor) {
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, defaultTimestampExtractor);
        return this;
    }

    /**
     * Defines which store implementations to plug in to DSL operators. Must implement the <code>org.apache.kafka.streams.state.DslStoreSuppliers</code> interface.
     */
    public StreamsConfigBuilder withDSLStoreSupplierClass(Class<? extends DslStoreSuppliers> dslStoreSupplierClass) {
        props.put(StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG, dslStoreSupplierClass);
        return this;
    }

    /**
     * The number of standby replicas for each task.
     */
    public StreamsConfigBuilder withNumStandbyReplicas(int numStandbyReplicas) {
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
        return this;
    }

    /**
     * The number of threads to execute stream processing.
     */
    public StreamsConfigBuilder withNumStreamThreads(int numStreamThreads) {
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        return this;
    }

    /**
     * Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records,
     * to avoid potential out-of-order record processing across multiple input streams.
     */
    public StreamsConfigBuilder withMaxTaskIdle(Duration maxTaskIdle) {
        props.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, maxTaskIdle.toMillis());
        return this;
    }

    /**
     * The processing guarantee that should be used. Possible values are <code>AT_LEAST_ONCE</code> (default) and <code>EXACTLY_ONCE</code>.
     * Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting
     * <code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.
     */
    public StreamsConfigBuilder withProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee.name);
        return this;
    }

    /**
     * List of client tag keys used to distribute standby replicas across Kafka Streams instances.
     * When configured, Kafka Streams will make a best-effort to distribute
     * the standby tasks over each client tag dimension.
     */
    public StreamsConfigBuilder withRackAwareAssignmentTags(List<String> rackAwareAssignmentTags) {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, rackAwareAssignmentTags);
        return this;
    }

    /**
     * The maximum amount of time in milliseconds a task might stall due to internal errors and retries until an error is raised.
     * For a timeout of 0ms, a task would raise an error for the first internal error.
     * For any timeout larger than 0ms, a task will retry at least once before an error is raised.
     */
    public StreamsConfigBuilder withTaskTimeout(Duration taskTimeout) {
        props.put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, taskTimeout.toMillis());
        return this;
    }

    /**
     * Sets window size for the deserializer in order to calculate window end times.
     */
    public StreamsConfigBuilder withWindowSize(Duration windowSize) {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, windowSize.toMillis());
        return this;
    }

    /**
     * A configuration telling Kafka Streams if it should optimize the topology, disabled by default
     */
    public StreamsConfigBuilder withTopologyOptimization(TopologyOptimization topologyOptimization) {
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, topologyOptimization.name);
        return this;
    }

    /**
     * A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application
     */
    public StreamsConfigBuilder withApplicationServer(String applicationServer) {
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, applicationServer);
        return this;
    }

    /**
     * Maximum number of records to buffer per partition.
     */
    public StreamsConfigBuilder withBufferedRecordsPerPartition(int bufferedRecordsPerPartition) {
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, bufferedRecordsPerPartition);
        return this;
    }

    /**
     * The frequency with which to save the position of the processor.
     * (Note, if <code>processing.guarantee</code> is set to <code>EXACTLY_ONCE</code>, the default value is <code>EOS_DEFAULT_COMMIT_INTERVAL_MS</code>,
     * otherwise the default value is <code>DEFAULT_COMMIT_INTERVAL_MS"</code>.
     */
    public StreamsConfigBuilder withCommitInterval(Duration commitInterval) {
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitInterval.toMillis());
        return this;
    }

    /**
     * The amount of time to block waiting for input.
     */
    public StreamsConfigBuilder withPoll(Duration poll) {
        props.put(StreamsConfig.POLL_MS_CONFIG, poll.toMillis());
        return this;
    }

    /**
     * A Rocks DB config setter class or class name that implements the <code>org.apache.kafka.streams.state.RocksDBConfigSetter</code> interface
     */
    public StreamsConfigBuilder withRocksDBConfigSetter(Class<? extends RocksDBConfigSetter> rocksDBConfigSetter) {
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, rocksDBConfigSetter);
        return this;
    }

    /**
     * The amount of time in milliseconds to wait before deleting state when a partition has migrated.
     * Only state directories that have not been modified for at least <code>state.cleanup.delay.ms</code> will be removed
     */
    public StreamsConfigBuilder withStateCleanupDelay(Duration stateCleanupDelay) {
        props.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, stateCleanupDelay.toMillis());
        return this;
    }

    /**
     * Allows upgrading from versions 0.10.0/0.10.1/0.10.2/0.11.0/1.0/1.1 to version 1.2 (or newer) in a backward compatible way.
     * When upgrading from 1.2 to a newer version it is not required to specify this config." +
     * Default is null. Accepted values are UPGRADE_FROM_0100, UPGRADE_FROM_0101, UPGRADE_FROM_0102, UPGRADE_FROM_0110, UPGRADE_FROM_10, UPGRADE_FROM_11(for upgrading from the corresponding old version).
     */
    public StreamsConfigBuilder withUpgradeFrom(UpgradeFrom upgradeFrom) {
        props.put(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom == null ? null : upgradeFrom.version);
        return this;
    }

    /**
     * Client supplier class that implements the <code>org.apache.kafka.streams.KafkaClientSupplier</code> interface.
     */
    public StreamsConfigBuilder withDefaultClientSupplier(Class<? extends KafkaClientSupplier> defaultClientSupplier) {
        props.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, defaultClientSupplier);
        return this;
    }

    /**
     * The strategy we use for rack aware assignment. Rack aware assignment will take <code>client.rack</code> and <code>racks</code> of <code>TopicPartition</code> into account when assigning
     * tasks to minimize cross rack traffic. Valid settings are : <code>"RACK_AWARE_ASSIGNMENT_STRATEGY_NONE</code> (default), which will disable rack aware assignment; <code>RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC</code>,
     * which will compute minimum cross rack traffic assignment; <code>"RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY"</code>, which will compute minimum cross rack traffic and try to balance the tasks of same subtopolgies across different clients
     */
    public StreamsConfigBuilder withRackAwareAssignmentStrategy(String rackAwareAssignmentStrategy) {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, rackAwareAssignmentStrategy);
        return this;
    }

    /**
     * Cost associated with cross rack traffic. This config and <code>rack.aware.assignment.non_overlap_cost</code> controls whether the
     * optimization algorithm favors minimizing cross rack traffic or minimize the movement of tasks in existing assignment. If set a larger value <code>RackAwareTaskAssignor.class.getName()</code> will
     * optimize for minimizing cross rack traffic. The default value is null which means it will use default traffic cost values in different assignors.
     */
    public StreamsConfigBuilder withRackAwareAssignmentTrafficCost(int rackAwareAssignmentTrafficCost) {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG, rackAwareAssignmentTrafficCost);
        return this;
    }

    /**
     * Cost associated with moving tasks from existing assignment. This config and <code>RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG</code> controls whether the
     * optimization algorithm favors minimizing cross rack traffic or minimize the movement of tasks in existing assignment. If set a larger value <code>RackAwareTaskAssignor.class.getName()</code> will
     * optimize to maintain the existing assignment. The default value is null which means it will use default non_overlap cost values in different assignors.
     */
    public StreamsConfigBuilder withRackAwareAssignmentNonOverlapCost(int rackAwareAssignmentNonOverlapCost) {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG, rackAwareAssignmentNonOverlapCost);
        return this;
    }

    /**
     * A task assignor class or class name implementing the <code>TaskAssignor.class.getName()</code> interface. Defaults to the <code>HighAvailabilityTaskAssignor</code> class.
     */
    public StreamsConfigBuilder withTaskAssignorClass(Class<? extends TaskAssignor> taskAssignorClass) {
        props.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, taskAssignorClass.getName());
        return this;
    }


    /**
     * Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day
     */
    public StreamsConfigBuilder withWindowStoreChangeLogAdditionalRetention(Duration windowStoreChangeLogAdditionalRetention) {
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, windowStoreChangeLogAdditionalRetention.toMillis());
        return this;
    }

    /**
     * The maximum acceptable lag (number of offsets to catch up) for a client to be considered caught-up for an active task.
     * Should correspond to a recovery time of well under a minute for a given workload. Must be at least 0.
     */
    public StreamsConfigBuilder withAcceptableRecoveryLag(long acceptableRecoveryLag) {
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, acceptableRecoveryLag);
        return this;
    }

    /**
     * The maximum number of warmup replicas (extra standbys beyond the configured num.standbys) that can be assigned at once for the purpose of keeping
     * the task available on one instance while it is warming up on another instance it has been reassigned to. Used to throttle how much extra broker
     * traffic and cluster state can be used for high availability. Must be at least 1.
     */
    public StreamsConfigBuilder withMaxWarmupReplicas(int maxWarmupReplicas) {
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, maxWarmupReplicas);
        return this;
    }

    /**
     * The maximum time to wait before triggering a rebalance to probe for warmup replicas that have finished warming up and are ready to become active. Probing rebalances
     * will continue to be triggered until the assignment is balanced. Must be at least 1 minute.
     */
    public StreamsConfigBuilder withProbingRebalanceInterval(Duration probingRebalanceInterval) {
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, probingRebalanceInterval.toMillis());
        return this;
    }
}
