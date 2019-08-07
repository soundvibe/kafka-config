package net.soundvibe.kafka.config.producer;

public enum Acks {

    ZERO("0"), ONE("1"), ALL("all");

    public final String name;

    Acks(String name) {
        this.name = name;
    }
}
