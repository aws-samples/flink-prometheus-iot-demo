package com.amazonaws.examples.flink.monitor;

import org.apache.flink.api.common.functions.MapFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass-through MapFunction that logs every event to TRACE level.
 * <p>
 * For development and testing only.
 * Logging every record adds A LOT of overhead. Do not use in production.
 */
public class EventTraceLogger<E> implements MapFunction<E, E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventTraceLogger.class);
    private final String logMessagePrefix;

    public EventTraceLogger(String logMessagePrefix) {
        this.logMessagePrefix = logMessagePrefix;
    }

    @Override
    public E map(E event) throws Exception {
        LOGGER.trace("{}: {}", logMessagePrefix, event);
        return event;
    }
}
