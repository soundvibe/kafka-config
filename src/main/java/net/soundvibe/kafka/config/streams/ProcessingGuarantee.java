package net.soundvibe.kafka.config.streams;

import org.apache.kafka.streams.StreamsConfig;

public enum ProcessingGuarantee {

    AT_LEAST_ONCE(StreamsConfig.AT_LEAST_ONCE), EXACTLY_ONCE(StreamsConfig.EXACTLY_ONCE);

    public final String name;

    ProcessingGuarantee(String name) {
        this.name = name;
    }
}
