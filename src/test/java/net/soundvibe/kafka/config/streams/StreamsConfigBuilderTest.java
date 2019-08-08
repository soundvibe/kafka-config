package net.soundvibe.kafka.config.streams;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class StreamsConfigBuilderTest {

    private static final String BOOTSTRAP_SERVERS = "http://localhost:9876";

    @Test
    void should_build_all_properties() throws IOException {
        Properties streamProps = StreamsConfigBuilder.create()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withRequestTimeout(Duration.ofSeconds(30))
                .withClientId("clientId")
                .withClientDNSLookup(ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY)
                .withClientRack("rack")
                .withConnectionsMaxIdle(Duration.ofSeconds(30))
                .withMetadataMaxAge(Duration.ofMinutes(3))
                .withReceiveBufferBytes(1024)
                .withMetricsNumSamples(100)
                .withMetricsRecordingLevel(Sensor.RecordingLevel.INFO)
                .withReconnectBackoff(Duration.ofSeconds(3))
                .withReconnectBackoffMax(Duration.ofSeconds(60))
                .withRetryBackoff(Duration.ofSeconds(5))
                .withSendBufferBytes(1024)
                .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .withApplicationId("appId")
                .withBufferedRecordsPerPartition(1500)
                .withApplicationServer("http://localhost:80")
                .withCacheMaxBytesBuffering(1024 * 100)
                .withCommitInterval(Duration.ofMillis(10))
                .withDefaultDeserializationExceptionHandler(TestDeserializationExceptionHandler.class)
                .withDefaultKeySerde(Serdes.StringSerde.class)
                .withDefaultValueSerde(Serdes.StringSerde.class)
                .withDefaultProductionExceptionHandler(TestProductionExceptionHandler.class)
                .withDefaultTimestampExtractor(TestTimestampExtractor.class)
                .withMaxTaskIdle(Duration.ofSeconds(60))
                .withNumStandbyReplicas(3)
                .withNumStreamThreads(4)
                .withPartitionGrouper(TestPartitionGrouper.class)
                .withPoll(Duration.ofSeconds(30))
                .withProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .withReplicationFactor(3)
                .withRocksDBConfigSetter(TestRocksDBConfigSetter.class)
                .withStateCleanupDelay(Duration.ofSeconds(5))
                .withStateDir(Files.createTempDirectory("tmp"))
                .withTopologyOptimization(TopologyOptimization.OPTIMIZE)
                .withUpgradeFrom(UpgradeFrom.UPGRADE_FROM_11)
                .withWindowStoreChangeLogAdditionalRetention(Duration.ofMinutes(5))
                .buildProperties();

        assertEquals(BOOTSTRAP_SERVERS, streamProps.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertValid(streamProps);
    }

    private void assertValid(Properties properties) {
        assertDoesNotThrow(() -> new KafkaStreams(new StreamsBuilder().build(), properties));
    }
}