package net.soundvibe.kafka.config.consumer;

import org.apache.kafka.common.security.auth.SecurityProviderCreator;
import sun.security.pkcs11.SunPKCS11;

import java.security.Provider;

public class TestSecurityProvider implements SecurityProviderCreator {
    @Override
    public Provider getProvider() {
        return new SunPKCS11();
    }
}
