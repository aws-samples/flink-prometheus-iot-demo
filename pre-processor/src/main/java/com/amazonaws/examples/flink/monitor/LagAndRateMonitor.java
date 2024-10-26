package com.amazonaws.examples.flink.monitor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;

/**
 * Pass-through MapFunction that emits lag and event rate as custom metrics.
 */
public class LagAndRateMonitor<E> extends RichMapFunction<E, E> {

    private transient long lag;
    private transient Meter meter;

    private final EventTimeExtractor<E> eventTimeExtractor;

    public LagAndRateMonitor(EventTimeExtractor<E> eventTimeExtractor) {
        this.eventTimeExtractor = eventTimeExtractor;
    }

    @Override
    public void open(OpenContext openContext) {
        getRuntimeContext()
                .getMetricGroup()
                .addGroup("kinesisAnalytics")
                .gauge("lagMs", (Gauge<Long>) () -> lag);

        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("kinesisAnalytics")
                .meter("eventRate", new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public E map(E event) {

        // Measure lag
        this.lag = System.currentTimeMillis() - eventTimeExtractor.apply(event);

        // Measure event rate
        this.meter.markEvent();

        // Pass-through the event, but update the lag metric
        return event;
    }

}



