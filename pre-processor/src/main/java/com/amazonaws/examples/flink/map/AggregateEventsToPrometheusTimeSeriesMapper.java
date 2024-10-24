package com.amazonaws.examples.flink.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;

public class AggregateEventsToPrometheusTimeSeriesMapper implements MapFunction<AggregateVehicleEvent, PrometheusTimeSeries> {
    @Override
    public PrometheusTimeSeries map(AggregateVehicleEvent aggregateEvent) throws Exception {
        return PrometheusTimeSeries.builder()
                .withMetricName(aggregateEvent.getEventType().name().toLowerCase())
                .addLabel("model", aggregateEvent.getVehicleModel())
                .addLabel("region", aggregateEvent.getRegion())
                .addSample(aggregateEvent.getCount(), aggregateEvent.getTimestamp())
                .build();
    }
}
