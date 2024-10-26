package com.amazonaws.examples.flink.domain;

/**
 * Type of vehicle event
 */
public enum EventType {
    IC_RPM,
    ELECTRIC_RPM,
    WARNINGS;

    public static final EventType[] EVENT_TYPES = {IC_RPM, ELECTRIC_RPM, WARNINGS};
}
