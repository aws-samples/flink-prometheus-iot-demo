package com.amazonaws.examples.flink.monitor;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Interface for extracting the event time from an event. Used by {@link LagAndRateMonitor}
 */
@FunctionalInterface
public interface EventTimeExtractor<E> extends Function<E,Long>, Serializable {
}
