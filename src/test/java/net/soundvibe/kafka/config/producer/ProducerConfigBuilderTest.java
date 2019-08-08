package net.soundvibe.kafka.config.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ProducerConfigBuilderTest {

    private static final String BOOTSTRAP_SERVERS = "http://localhost:9876";

    @Test
    void should_build_all_properties() {
        Map<String, Object> producerProps = ProducerConfigBuilder.create()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withAcks(Acks.ALL)
                .withBatchSize(10)
                .withCompressionType(CompressionType.SNAPPY)
                .withEnableIdempotence(true)
                .withBufferMemory(1024L * 1000L)
                .withDeliveryTimeout(Duration.ofMinutes(10))
                .withKeySerializer(StringSerializer.class)
                .withValueSerializer(StringSerializer.class)
                .withLinger(Duration.ofMinutes(1))
                .withMaxBlock(Duration.ofSeconds(10))
                .withPartitioner(DefaultPartitioner.class)
                .withMaxRequestSize(1024)
                .withRetries(10)
                .withTransactionalId("transactionalId")
                .withTransactionTimeout(Duration.ofSeconds(30))
                .withRequestTimeout(Duration.ofSeconds(30))
                .withClientId("clientId")
                .withMaxInFlightRequestsPerConnection(5)
                .withInterceptorClasses(TestProducerInterceptor.class)
                .buildMap();

        assertEquals(BOOTSTRAP_SERVERS, producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertValid(producerProps);
    }

    private void assertValid(Map<String, Object> properties) {
        assertDoesNotThrow(() -> new KafkaProducer<>(properties));
    }
}