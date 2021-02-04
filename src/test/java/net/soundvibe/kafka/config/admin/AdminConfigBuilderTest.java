package net.soundvibe.kafka.config.admin;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.metrics.Sensor;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class AdminConfigBuilderTest {

    private static final String BOOTSTRAP_SERVERS = "http://localhost:9876";

    @Test
    void should_build_all_properties() {
        Properties adminProps = AdminConfigBuilder.create()
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
                .withCustomMap(new HashMap<>())
				.withSocketConnectionSetupTimeout(Duration.ofSeconds(15))
				.withSocketConnectionSetupTimeoutMax(Duration.ofSeconds(30))
                .buildProperties();

        assertEquals(BOOTSTRAP_SERVERS, adminProps.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertValid(adminProps);
    }

    private void assertValid(Properties properties) {
        assertDoesNotThrow(() -> AdminClient.create(properties));
    }
}
