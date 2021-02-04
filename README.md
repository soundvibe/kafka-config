[![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.soundvibe/kafka-config/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.soundvibe/kafka-config)
[![Build Status](https://travis-ci.org/soundvibe/kafka-config.png)](https://travis-ci.org/soundvibe/kafka-config)
[![Coverage Status](https://codecov.io/github/soundvibe/kafka-config/coverage.svg?branch=master)](https://codecov.io/github/soundvibe/kafka-config?branch=master)

# kafka-config

Easy to use type-safe builders for kafka clients.
Supports Java >= 8.
kafka-config version aligns with kafka-client version.

## Motivation

Configuring kafka consumers and producers is usually quite error-prone task because kafka client relies on `java.util.Properties` class.
This introduces such issues as the lack of type-safety, possible mi-spelling of names, lack of auto-completion and easy to find javadoc.
`kafka-config` tries to address these issues by providing simple builders to construct kafka configuration easier and safer.
Also, listing and searching for all available configuration parameters is much easier because of auto-completion.

Supported clients:
 * KafkaConsumer
 * KafkaProducer
 * KafkaStreams
 * AdminClient

## Examples
```java
public static KafkaConsumer<String,String> createConsumer() {
  //provided configuration values are random, do not copy them
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
      .buildProperties();
  return new KafkaConsumer<>(consumerProps);
}

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
```

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at [http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cnet.soundvibe.kafka-config).

Example for Gradle:

```groovy
compile 'net.soundvibe:kafka-config:2.6.1'
```

and for Maven:

```xml
<dependency>
    <groupId>net.soundvibe</groupId>
    <artifactId>kafka-config</artifactId>
    <version>2.6.1</version>
</dependency>
```


## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/soundvibe/kafka-config/issues).

## LICENSE

Copyright 2019 Linas Naginionis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

