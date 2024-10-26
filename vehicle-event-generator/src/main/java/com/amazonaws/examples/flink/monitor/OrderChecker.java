package com.amazonaws.examples.flink.monitor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class OrderChecker<K, E> extends KeyedProcessFunction<K, E, E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderChecker.class);
    private transient ValueState<Long> lastRecordTimestampState;

    private final Function<E, Long> timestampExtractor;

    public OrderChecker(Function<E, Long> timestampExtractor) {
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("lastRecordTimestamp", Long.class);
        lastRecordTimestampState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(E value, KeyedProcessFunction<K, E, E>.Context ctx, Collector<E> out) throws Exception {
        Long prevRecordTimestamp = lastRecordTimestampState.value();
        if (prevRecordTimestamp == null) {
            prevRecordTimestamp = 0L;
        }
        long currentTimestamp = timestampExtractor.apply(value);
        if (currentTimestamp > 0 && currentTimestamp <= prevRecordTimestamp) {
            LOGGER.warn("Detected out of order record: curr ts {}. Prev ts {}, key {}", value, prevRecordTimestamp, ctx.getCurrentKey());
        }
        lastRecordTimestampState.update(Math.max(currentTimestamp, prevRecordTimestamp));
        out.collect(value);
    }
}
