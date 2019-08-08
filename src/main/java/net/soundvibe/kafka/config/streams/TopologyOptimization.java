package net.soundvibe.kafka.config.streams;

import org.apache.kafka.streams.StreamsConfig;

public enum TopologyOptimization {

    NO_OPTIMIZATION(StreamsConfig.NO_OPTIMIZATION), OPTIMIZE(StreamsConfig.OPTIMIZE);

    public final String name;

    TopologyOptimization(String name) {
        this.name = name;
    }
}
