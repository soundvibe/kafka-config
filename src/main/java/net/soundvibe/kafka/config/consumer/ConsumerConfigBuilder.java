package net.soundvibe.kafka.config.consumer;

import net.soundvibe.kafka.config.AbstractConfigBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;

import java.time.Duration;
import java.util.*;

public final class ConsumerConfigBuilder extends AbstractConfigBuilder<ConsumerConfigBuilder> {

    private ConsumerConfigBuilder() {}

    public static ConsumerConfigBuilder create() {
        return new ConsumerConfigBuilder();
    }


    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     * This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.
     */
    public ConsumerConfigBuilder withGroupId(String groupId) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return this;
    }

    /**
     * A unique identifier of the consumer instance provided by end user.
     * Only non-empty strings are permitted. If set, the consumer is treated as a static member,
     * which means that only one instance with this ID is allowed in the consumer group at any time.
     * This can be used in combination with a larger session timeout to avoid group rebalances caused by transient unavailability
     * (e.g. process restarts). If not set, the consumer will join the group as a dynamic member, which is the traditional behavior.
     */
    public ConsumerConfigBuilder withGroupInstanceId(String groupInstanceId) {
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        return this;
    }

    /**
     * The maximum number of records returned in a single call to poll().
     */
    public ConsumerConfigBuilder withMaxPollRecords(int maxPollRecords) {
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        return this;
    }

    /**
     * The maximum delay between invocations of poll() when using
     * consumer group management. This places an upper bound on the amount of time that the consumer can be idle
     * before fetching more records. If poll() is not called before expiration of this timeout, then the consumer
     * is considered failed and the group will rebalance in order to reassign the partitions to another member.
     */
    public ConsumerConfigBuilder withMaxPollInterval(Duration maxPollInterval) {
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) maxPollInterval.toMillis());
        return this;
    }

    /**
     * The timeout used to detect consumer failures when using
     * Kafka's group management facility. The consumer sends periodic heartbeats to indicate its liveness
     * to the broker. If no heartbeats are received by the broker before the expiration of this session timeout,
     * then the broker will remove this consumer from the group and initiate a rebalance. Note that the value
     * must be in the allowable range as configured in the broker configuration by <code>group.min.session.timeout.ms</code>
     * and <code>group.max.session.timeout.ms</code>.
     */
    public ConsumerConfigBuilder withSessionTimeout(Duration sessionTimeout) {
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) sessionTimeout.toMillis());
        return this;
    }

    /**
     * The expected time between heartbeats to the consumer
     * coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the
     * consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group.
     * The value must be set lower than <code>session.timeout.ms</code>, but typically should be set no higher
     * than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
     */
    public ConsumerConfigBuilder withHeartbeatInterval(Duration heartbeatInterval) {
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, (int) heartbeatInterval.toMillis());
        return this;
    }

    /**
     * If true the consumer's offset will be periodically committed in the background.
     */
    public ConsumerConfigBuilder withEnableAutoCommit(boolean enableAutoCommit) {
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        return this;
    }

    /**
     * The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.
     */
    public ConsumerConfigBuilder withAutoCommitInterval(Duration autoCommitInterval) {
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, (int) autoCommitInterval.toMillis());
        return this;
    }

    /**
     * A list of class names or class types, ordered by preference, of supported assignors responsible for the partition assignment strategy that the client will use to distribute partition ownership amongst consumer instances when group management is used. Implementing the <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code> interface allows you to plug in a custom assignment strategy.
     */
    @SafeVarargs
    public final ConsumerConfigBuilder withPartitionAssignmentStrategy(Class<? extends ConsumerPartitionAssignor>... partitionAssignmentStrategy) {
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, Arrays.asList(partitionAssignmentStrategy));
        return this;
    }

    /**
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the offset to the earliest offset<li>latest: automatically reset the offset to the latest offset</li><li>none: throw exception to the consumer if no previous offset is found for the consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>
     */
    public ConsumerConfigBuilder withAutoOffsetReset(OffsetResetStrategy autoOffsetReset) {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.name().toLowerCase(Locale.ROOT));
        return this;
    }

    /**
     * The minimum amount of data the server should return for a fetch request.
     * If insufficient data is available the request will wait for that much data to accumulate before answering the request.
     * The default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is available or the fetch request times out waiting for data to arrive.
     * Setting this to something greater than 1 will cause the server to wait for larger amounts of data to accumulate which can improve server throughput a bit at the cost of some additional latency.
     */
    public ConsumerConfigBuilder withFetchMinBytes(int fetchMinBytes) {
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        return this;
    }

    /**
     * The maximum amount of data the server should return for a fetch request.
     * Records are fetched in batches by the consumer, and if the first record batch in the first non-empty partition of the fetch is larger than
     * this value, the record batch will still be returned to ensure that the consumer can make progress. As such, this is not a absolute maximum.
     * The maximum record batch size accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or
     * <code>max.message.bytes</code> (topic config). Note that the consumer performs multiple fetches in parallel.
     */
    public ConsumerConfigBuilder withFetchMaxBytes(int fetchMaxBytes) {
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        return this;
    }

    /**
     * The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by fetch.min.bytes.
     */
    public ConsumerConfigBuilder withFetchMaxWait(Duration fetchMaxWait) {
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, (int) fetchMaxWait.toMillis());
        return this;
    }

    /**
     * The maximum amount of data per-partition the server
     * will return. Records are fetched in batches by the consumer. If the first record batch in the first non-empty
     * partition of the fetch is larger than this limit, the
     * batch will still be returned to ensure that the consumer can make progress. The maximum record batch size
     * accepted by the broker is defined via <code>message.max.bytes</code> (broker config) or
     * <code>max.message.bytes</code> (topic config). See FETCH_MAX_BYTES_CONFIG for limiting the consumer request size.
     */
    public ConsumerConfigBuilder withMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        return this;
    }

    /**
     * Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk corruption to the messages occurred.
     * This check adds some overhead, so it may be disabled in cases seeking extreme performance.
     */
    public ConsumerConfigBuilder withCheckCRCs(boolean checkCRCs) {
        props.put(ConsumerConfig.CHECK_CRCS_CONFIG, checkCRCs);
        return this;
    }

    /**
     * Deserializer class for key that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.
     */
    public ConsumerConfigBuilder withKeyDeserializer(Class<? extends Deserializer> keyDeserializer) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        return this;
    }

    /**
     * Deserializer class for value that implements the <code>org.apache.kafka.common.serialization.Deserializer</code> interface.
     */
    public ConsumerConfigBuilder withValueDeserializer(Class<? extends Deserializer> valueDeserializer) {
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        return this;
    }

    /**
     * Specifies the timeout for consumer APIs that could block.
     * This configuration is used as the default timeout for all consumer operations that do not explicitly accept a <code>timeout</code> parameter.
     */
    public ConsumerConfigBuilder withDefaultApiTimeout(Duration defaultApiTimeout) {
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) defaultApiTimeout.toMillis());
        return this;
    }

    /**
     * A list of classes to use as interceptors.
     * Implementing the <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records
     * received by the consumer. By default, there are no interceptors.
     */
    @SafeVarargs
    public final ConsumerConfigBuilder withInterceptorClasses(Class<? extends ConsumerInterceptor>... interceptorClasses) {
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList(interceptorClasses));
        return this;
    }

    /**
     * Whether internal topics matching a subscribed pattern should
     * be excluded from the subscription. It is always possible to explicitly subscribe to an internal topic.
     */
    public ConsumerConfigBuilder withExcludeInternalTopics(boolean excludeInternalTopics) {
        props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopics);
        return this;
    }

    /**
     * <p>Controls how to read messages written transactionally. If set to <code>read_committed</code>, consumer.poll() will only return
     * transactional messages which have been committed. If set to <code>read_uncommitted</code>' (the default), consumer.poll() will return all messages, even transactional messages
     * which have been aborted. Non-transactional messages will be returned unconditionally in either mode.</p> <p>Messages will always be returned in offset order. Hence, in
     * <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable offset (LSO), which is the one less than the offset of the first open transaction.
     * In particular any messages appearing after messages belonging to ongoing transactions will be withheld until the relevant transaction has been completed. As a result, <code>read_committed</code>
     * consumers will not be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when in <code>read_committed</code> the seekToEnd method will
     * return the LSO
     */
    public ConsumerConfigBuilder withIsolationLevel(IsolationLevel isolationLevel) {
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.toString().toLowerCase(Locale.ROOT));
        return this;
    }

    /**
     * Allow automatic topic creation on the broker when
     * subscribing to or assigning a topic. A topic being subscribed to will be automatically created only if the
     * broker allows for it using `auto.create.topics.enable` broker configuration. This configuration must
     * be set to `false` when using brokers older than 0.11.0
     */
    public ConsumerConfigBuilder withAllowAutoCreateTopics(boolean allowAutoCreateTopics) {
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopics);
        return this;
    }
}
