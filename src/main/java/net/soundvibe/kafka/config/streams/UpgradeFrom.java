package net.soundvibe.kafka.config.streams;

import org.apache.kafka.streams.StreamsConfig;

public enum UpgradeFrom {

    UPGRADE_FROM_0100(StreamsConfig.UPGRADE_FROM_0100),
    UPGRADE_FROM_0101(StreamsConfig.UPGRADE_FROM_0101),
    UPGRADE_FROM_0102(StreamsConfig.UPGRADE_FROM_0102),
    UPGRADE_FROM_0110(StreamsConfig.UPGRADE_FROM_0110),
    UPGRADE_FROM_10(StreamsConfig.UPGRADE_FROM_10),
    UPGRADE_FROM_11(StreamsConfig.UPGRADE_FROM_11);

    public final String version;

    UpgradeFrom(String version) {
        this.version = version;
    }
}
