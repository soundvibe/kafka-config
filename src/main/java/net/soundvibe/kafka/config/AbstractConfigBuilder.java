package net.soundvibe.kafka.config;

import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.*;

import java.time.Duration;
import java.util.*;

import static java.util.stream.Collectors.joining;

@SuppressWarnings("unchecked")
public class AbstractConfigBuilder<T extends AbstractConfigBuilder<T>> implements ConfigBuilder {

    protected final Map<String, Object> props = new LinkedHashMap<>();

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form
     * <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to
     * discover the full cluster membership (which may change dynamically), this list need not contain the full set of
     * servers (you may want more than one, though, in case a server is down).
     */
    public T withBootstrapServers(String bootstrapServers) {
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return (T) this;
    }

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
     * The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form
     * <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to
     * discover the full cluster membership (which may change dynamically), this list need not contain the full set of
     * servers (you may want more than one, though, in case a server is down).
     */
    public T withBootstrapServers(List<String> bootstrapServers) {
        return withBootstrapServers(String.join(",", bootstrapServers));
    }

    /**
     * An id string to pass to the server when making requests.
     * The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.
     */
    public T withClientId(String clientId) {
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        return (T) this;
    }

    /**
     * A rack identifier for this client. This can be any string value which indicates where this client is physically located.
     * It corresponds with the broker config 'broker.rack'
     */
    public T withClientRack(String clientRack) {
        props.put(CommonClientConfigs.CLIENT_RACK_CONFIG, clientRack);
        return (T) this;
    }

    /**
     * <p>Controls how the client uses DNS lookups.</p><p>If set to <code>use_all_dns_ips</code> then, when the lookup returns multiple IP addresses for a hostname,
     * they will all be attempted to connect to before failing the connection. Applies to both bootstrap and advertised servers.</p>
     * <p>If the value is <code>resolve_canonical_bootstrap_servers_only</code> each entry will be resolved and expanded into a list of canonical names.</p>
     */
    public T withClientDNSLookup(ClientDnsLookup clientDNSLookup) {
        props.put(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, clientDNSLookup.toString());
        return (T) this;
    }

    /**
     * The period of time after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.
     */
    public T withMetadataMaxAge(Duration metadataMaxAge) {
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, (int) metadataMaxAge.toMillis());
        return (T) this;
    }

    /**
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.
     */
    public T withSendBufferBytes(int sendBufferBytes) {
        props.put(CommonClientConfigs.SEND_BUFFER_CONFIG, sendBufferBytes);
        return (T) this;
    }

    /**
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.
     */
    public T withReceiveBufferBytes(int receiveBufferBytes) {
        props.put(CommonClientConfigs.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
        return (T) this;
    }

    /**
     * The base amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop.
     * This backoff applies to all connection attempts by the client to a broker.
     */
    public T withReconnectBackoff(Duration reconnectBackoff) {
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoff.toMillis());
        return (T) this;
    }

    /**
     * The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect.
     * If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum.
     * After calculating the backoff increase, 20% random jitter is added to avoid connection storms.
     */
    public T withReconnectBackoffMax(Duration reconnectBackoffMax) {
        props.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMax.toMillis());
        return (T) this;
    }

    /**
     * The amount of time to wait before attempting to retry a failed request to a given topic partition.
     * This avoids repeatedly sending requests in a tight loop under some failure scenarios.
     */
    public T withRetryBackoff(Duration retryBackoff) {
        props.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoff.toMillis());
        return (T) this;
    }

    /**
     * The window of time a metrics sample is computed over.
     */
    public T withMetricsSampleWindow(Duration metricsSampleWindow) {
        props.put(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindow.toMillis());
        return (T) this;
    }

    /**
     * The number of samples maintained to compute metrics.
     */
    public T withMetricsNumSamples(int metricsNumSamples) {
        props.put(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
        return (T) this;
    }

    /**
     * The highest recording level for metrics.
     */
    public T withMetricsRecordingLevel(Sensor.RecordingLevel metricsRecordingLevel) {
        props.put(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel.name);
        return (T) this;
    }

    /**
     * A list of classes to use as metrics reporters.
     * Implementing the <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes that will be notified of new metric creation.
     * The JmxReporter is always included to register JMX statistics.
     */
    public T withMetricReporterClasses(Class<?>... metricReporterClasses) {
        props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, Arrays.asList(metricReporterClasses));
        return (T) this;
    }

    /**
     * Close idle connections after the duration specified by this config.
     */
    public T withConnectionsMaxIdle(Duration connectionsMaxIdle) {
        props.put(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdle.toMillis());
        return (T) this;
    }

    /**
     * The configuration controls the maximum amount of time the client will wait
     * for the response of a request. If the response is not received before the timeout
     * elapses the client will resend the request if necessary or fail the request if
     * retries are exhausted.
     */
    public T withRequestTimeout(Duration requestTimeout) {
        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, (int) requestTimeout.toMillis());
        return (T) this;
    }

    /**
     * Protocol used to communicate with brokers.
     */
    public T withSecurityProtocol(SecurityProtocol securityProtocol) {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
        return (T) this;
    }

    /**
     * SASL mechanism used for client connections. This may be any mechanism for which a security provider is available. GSSAPI is the default mechanism.
     */
    public T withSaslMechanism(String saslMechanism) {
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        return (T) this;
    }

    //new in 2.4.0
    /**
     * A list of configurable creator classes each returning a provider
     * implementing security algorithms. These classes should implement the
     * <code>org.apache.kafka.common.security.auth.SecurityProviderCreator</code> interface.
     */
    @SafeVarargs
    public final T withSecurityProviders(Class<? extends SecurityProviderCreator>... securityProviders) {
        props.put(ConsumerConfig.SECURITY_PROVIDERS_CONFIG, Arrays.stream(securityProviders)
                .map(Class::getName)
                .collect(joining(",")));
        return (T) this;
    }

    /**
     * Use custom configuration property
     * @param name property name
     * @param value property value
     * @return this
     */
    public T withCustom(String name, String value) {
        props.put(name, value);
        return (T) this;
    }

    /**
     * Builds config into java.util.Properties class to use with Kafka consumers, producers or admin clients
     * @return properties
     */
    @Override
    public Properties buildProperties() {
        Properties properties = new Properties();
        props.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .forEach(e -> properties.put(e.getKey(), e.getValue()));
        return properties;
    }

    @Override
    public Map<String, Object> buildMap() {
        return new LinkedHashMap<>(props);
    }
}
