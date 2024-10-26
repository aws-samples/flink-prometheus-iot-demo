package com.amazonaws.examples.flink.map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.util.Collector;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;

public class AggregateEventsToPrometheusTimeSeriesMapper implements FlatMapFunction<AggregateVehicleEvent, PrometheusTimeSeries> {

    @Override
    public void flatMap(AggregateVehicleEvent aggregateEvent, Collector<PrometheusTimeSeries> out) throws Exception {
        // Only map non-empty aggregates
        if (!aggregateEvent.isEmpty()) {
            out.collect(PrometheusTimeSeries.builder()
                    .withMetricName(getMetricName(aggregateEvent))
                    .addLabel("model", aggregateEvent.getVehicleModel())
                    .addLabel("region", aggregateEvent.getRegion())
                    .addSample(aggregateEvent.getCount(), aggregateEvent.getTimestamp())
                    .build());
        }
    }

    private static String getMetricName(AggregateVehicleEvent aggregateEvent) {
        return aggregateEvent.getEventType().name().toLowerCase();
    }
}
