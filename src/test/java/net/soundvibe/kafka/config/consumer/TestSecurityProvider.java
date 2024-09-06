package net.soundvibe.kafka.config.consumer;

import org.apache.kafka.common.security.auth.SecurityProviderCreator;

import java.security.Provider;

public class TestSecurityProvider implements SecurityProviderCreator {
    @Override
    public Provider getProvider() {
        return new FooProvider();
    }
}


class FooProvider extends Provider {

    public FooProvider() {
        super("foo", "v1", "info");
    }

}