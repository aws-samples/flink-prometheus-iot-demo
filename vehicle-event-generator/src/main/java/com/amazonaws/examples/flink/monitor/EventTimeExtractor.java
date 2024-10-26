package com.amazonaws.examples.flink.monitor;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface EventTimeExtractor<E> extends Function<E,Long>, Serializable {
}
