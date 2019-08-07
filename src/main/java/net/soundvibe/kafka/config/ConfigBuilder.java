package net.soundvibe.kafka.config;

import java.util.*;

public interface ConfigBuilder {

    Properties buildProperties();

    Map<String, Object> buildMap();

}
