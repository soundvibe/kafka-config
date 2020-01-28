package net.soundvibe.kafka.config.consumer;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ConsumerConfigBuilderTest {

    @Test
    void should_set_group_id() {
        Properties properties = ConsumerConfigBuilder.create()
                .withGroupId("test-consumer")
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .buildProperties();

        assertEquals(BOOTSTRAP_SERVERS, properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-consumer", properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    private static final String BOOTSTRAP_SERVERS = "http://localhost:9876";
    private static final String BOOTSTRAP_SERVERS2 = "http://localhost:1234";

    @Test
    void should_set_bootstrap_servers() {
        Properties properties = ConsumerConfigBuilder.create()
                .withBootstrapServers(asList(BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS2))
                .withGroupId("test")
                .buildProperties();

        assertEquals(BOOTSTRAP_SERVERS + "," + BOOTSTRAP_SERVERS2, properties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertValid(properties);
    }

    @Test
    void should_set_all_props() {
        Properties consumerProps = ConsumerConfigBuilder.create()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withGroupId("test-group")
                .withAllowAutoCreateTopics(true)
                .withAutoOffsetReset(OffsetResetStrategy.EARLIEST)
                .withCheckCRCs(true)
                .withEnableAutoCommit(false)
                .withDefaultApiTimeout(Duration.ofMinutes(1))
                .withAutoCommitInterval(Duration.ofSeconds(30))
                .withExcludeInternalTopics(false)
                .withFetchMaxBytes(1024 * 10)
                .withFetchMaxWait(Duration.ofSeconds(60))
                .withFetchMinBytes(1024)
                .withGroupInstanceId("groupId")
                .withHeartbeatInterval(Duration.ofSeconds(10))
                .withInterceptorClasses(TestConsumerInterceptor.class)
                .withIsolationLevel(IsolationLevel.READ_COMMITTED)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withMaxPartitionFetchBytes(1024 * 100)
                .withMaxPollInterval(Duration.ofSeconds(30))
                .withMaxPollRecords(1000)
                .withPartitionAssignmentStrategy(RoundRobinAssignor.class)
                .withSessionTimeout(Duration.ofMinutes(5))
                .withClientDNSLookup(ClientDnsLookup.USE_ALL_DNS_IPS)
                .withClientId("clientId")
                .withClientRack("clientRack")
                .withConnectionsMaxIdle(Duration.ofSeconds(60))
                .withMetadataMaxAge(Duration.ofMinutes(5))
                .withMetricReporterClasses(JmxReporter.class)
                .withMetricsNumSamples(1000)
                .withMetricsRecordingLevel(Sensor.RecordingLevel.INFO)
                .withMetricsSampleWindow(Duration.ofSeconds(30))
                .withReceiveBufferBytes(1024)
                .withSendBufferBytes(1024)
                .withReconnectBackoff(Duration.ofSeconds(3))
                .withReconnectBackoffMax(Duration.ofMinutes(5))
                .withRequestTimeout(Duration.ofSeconds(10))
                .withRetryBackoff(Duration.ofSeconds(3))
                .withSecurityProviders(TestSecurityProvider.class)
                .withSaslMechanism("SCRAM-SHA-256")
                .withCustom("test", "test")
                .buildProperties();

        assertEquals(BOOTSTRAP_SERVERS, consumerProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertValidNoSerializers(consumerProps);
    }

    private void assertValid(Properties properties) {
        assertDoesNotThrow(() -> new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer()));
    }

    private void assertValidNoSerializers(Properties properties) {
        assertDoesNotThrow(() -> new KafkaConsumer<>(properties));
    }
}