package net.soundvibe.kafka.config.admin;

import net.soundvibe.kafka.config.AbstractConfigBuilder;

public final class AdminConfigBuilder extends AbstractConfigBuilder<AdminConfigBuilder> {

    private AdminConfigBuilder() {}

    public static AdminConfigBuilder create() {
        return new AdminConfigBuilder();
    }

}
