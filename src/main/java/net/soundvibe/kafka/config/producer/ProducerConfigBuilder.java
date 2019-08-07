package net.soundvibe.kafka.config.producer;

import net.soundvibe.kafka.config.AbstractConfigBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.Arrays;

public final class ProducerConfigBuilder extends AbstractConfigBuilder<ProducerConfigBuilder> {

    private ProducerConfigBuilder() {}

    public static ProducerConfigBuilder create() {
        return new ProducerConfigBuilder();
    }

    /**
     * The producer will attempt to batch records together into fewer requests whenever multiple records are being sent
     *  to the same partition. This helps performance on both the client and the server. This configuration controls the
     *  default batch size in bytes. <p>
     *  No attempt will be made to batch records larger than this size.<p>
     *  Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent.<p>
     *  A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable
     *  batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a
     *  buffer of the specified batch size in anticipation of additional records.
     */
    public ProducerConfigBuilder withBatchSize(int batchSize) {
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        return this;
    }

    /**
     * The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the
     * durability of records that are sent. The following settings are allowed: <ul><li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the
     * server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be
     * made that the server has received the record in this case, and the <code>retries</code> configuration will not
     * take effect (as the client won't generally know of any failures). The offset given back for each record will
     * always be set to -1.<li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond
     * without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after
     * acknowledging the record but before the followers have replicated it then the record will be lost.<li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to
     * acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica
     * remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting.
     */
    public ProducerConfigBuilder withAcks(Acks acks) {
        props.put(ProducerConfig.ACKS_CONFIG, acks.name);
        return this;
    }

    /**
     * The producer groups together any records that arrive in between request transmissions into a single batched request.
     * Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to
     * reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount
     * of artificial delay&mdash;that is, rather than immediately sending out a record the producer will wait for up to
     * the given delay to allow other records to be sent so that the sends can be batched together. This can be thought
     * of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once
     * we get <code>BATCH_SIZE_CONFIG</code> worth of records for a partition it will be sent immediately regardless of this
     * setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the
     * specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay). Setting <code>LINGER_MS_CONFIG=5</code>,
     * for example, would have the effect of reducing the number of requests sent but would add up to 5ms of latency to records sent in the absence of load.
     */
    public ProducerConfigBuilder withLinger(Duration linger) {
        props.put(ProducerConfig.LINGER_MS_CONFIG, linger.toMillis());
        return this;
    }

    /**
     * This should be larger than <code>replica.lag.time.max.ms</code> (a broker configuration)
     * to reduce the possibility of message duplication due to unnecessary producer retries.
     */
    @Override
    public ProducerConfigBuilder withRequestTimeout(Duration requestTimeout) {
        return super.withRequestTimeout(requestTimeout);
    }

    /**
     * An upper bound on the time to report success or failure
     * after a call to <code>send()</code> returns. This limits the total time that a record will be delayed
     * prior to sending, the time to await acknowledgement from the broker (if expected), and the time allowed
     * for retriable send failures. The producer may report failure to send a record earlier than this config if
     * either an unrecoverable error is encountered, the retries have been exhausted,
     * or the record is added to a batch which reached an earlier delivery expiration deadline.
     * The value of this config should be greater than or equal to the sum of <code> REQUEST_TIMEOUT_MS_CONFIG </code>
     * and <code> LINGER_MS_CONFIG </code>.
     */
    public ProducerConfigBuilder withDeliveryTimeout(Duration deliveryTimeout) {
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) deliveryTimeout.toMillis());
        return this;
    }

    /**
     * The maximum size of a request in bytes. This setting will limit the number of record
     * batches the producer will send in a single request to avoid sending huge requests.
     * This is also effectively a cap on the maximum record batch size. Note that the server
     * has its own cap on record batch size which may be different from this.
     */
    public ProducerConfigBuilder withMaxRequestSize(int maxRequestSize) {
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
        return this;
    }

    /**
     * The configuration controls how long <code>KafkaProducer.send()</code> and <code>KafkaProducer.partitionsFor()</code> will block.
     * These methods can be blocked either because the buffer is full or metadata unavailable.
     * Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.
     */
    public ProducerConfigBuilder withMaxBlock(Duration maxBlock) {
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlock.toMillis());
        return this;
    }

    /**
     * The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are
     * sent faster than they can be delivered to the server the producer will block for <code>MAX_BLOCK_MS_CONFIG</code> after which it will throw an exception.<p>
     * This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since
     * not all memory the producer uses is used for buffering. Some additional memory will be used for compression (if
     * compression is enabled) as well as for maintaining in-flight requests.
     */
    public ProducerConfigBuilder withBufferMemory(long bufferMemory) {
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        return this;
    }

    /**
     * The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid
     * values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or <code>zstd</code>.
     * Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).
     */
    public ProducerConfigBuilder withCompressionType(CompressionType compressionType) {
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType.name);
        return this;
    }

    /**
     * The maximum number of unacknowledged requests the client will send on a single connection before blocking.
     * Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of
     * message re-ordering due to retries (i.e., if retries are enabled).
     */
    public ProducerConfigBuilder withMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequestsPerConnection);
        return this;
    }

    /**
     * Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
     * Note that this retry is no different than if the client resent the record upon receiving the error.
     * Allowing retries without setting <code>MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION</code> to 1 will potentially change the
     * ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second
     * succeeds, then the records in the second batch may appear first. Note additionally that produce requests will be
     * failed before the number of retries has been exhausted if the timeout configured by
     * <code>DELIVERY_TIMEOUT_MS_CONFIG</code> expires first before successful acknowledgement. Users should generally
     * prefer to leave this config unset and instead use <code>DELIVERY_TIMEOUT_MS_CONFIG</code> to control retry behavior.
     */
    public ProducerConfigBuilder withRetries(int retries) {
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        return this;
    }

    /**
     * Partitioner class that implements the <code>org.apache.kafka.clients.producer.Partitioner</code> interface.
     */
    public ProducerConfigBuilder withPartitioner(Class<? extends Partitioner> partitioner) {
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        return this;
    }

    /**
     * When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer
     * retries due to broker failures, etc., may write duplicates of the retried message in the stream.
     * Note that enabling idempotence requires <code>MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION</code> to be less than or equal to 5,
     * <code>RETRIES_CONFIG</code> to be greater than 0 and <code>ACKS_CONFIG</code> must be 'all'. If these values
     * are not explicitly set by the user, suitable values will be chosen. If incompatible values are set,
     * a <code>ConfigException</code> will be thrown.
     */
    public ProducerConfigBuilder withEnableIdempotence(boolean enableIdempotence) {
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        return this;
    }

    /**
     * The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction.
     * If this value is larger than the transaction.max.timeout.ms setting in the broker, the request will fail with a <code>InvalidTransactionTimeout</code> error.
     */
    public ProducerConfigBuilder withTransactionTimeout(Duration transactionTimeout) {
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) transactionTimeout.toMillis());
        return this;
    }

    /**
     * The TransactionalId to use for transactional delivery. This enables reliability semantics which span multiple producer
     * sessions since it allows the client to guarantee that transactions using the same TransactionalId have been completed prior to starting any new transactions.
     * If no TransactionalId is provided, then the producer is limited to idempotent delivery.
     * Note that <code>enable.idempotence</code> must be enabled if a TransactionalId is configured.
     * The default is <code>null</code>, which means transactions cannot be used.
     * Note that, by default, transactions require a cluster of at least three brokers which is the recommended setting for production;
     * for development you can change this, by adjusting broker setting <code>transaction.state.log.replication.factor</code>.
     */
    public ProducerConfigBuilder withTransactionalId(String transactionalId) {
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return this;
    }

    /**
     * Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.
     */
    public ProducerConfigBuilder withKeySerializer(Serializer<?> keySerializer) {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        return this;
    }

    /**
     * Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.
     */
    public ProducerConfigBuilder withValueSerializer(Serializer<?> valueSerializer) {
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return this;
    }

    /**
     * A list of classes to use as interceptors.
     * Implementing the <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept (and possibly mutate) records
     * received by the consumer. By default, there are no interceptors.
     */
    @SafeVarargs
    public final ProducerConfigBuilder withInterceptorClasses(Class<? extends ProducerInterceptor>... interceptorClasses) {
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList(interceptorClasses));
        return this;
    }
}
