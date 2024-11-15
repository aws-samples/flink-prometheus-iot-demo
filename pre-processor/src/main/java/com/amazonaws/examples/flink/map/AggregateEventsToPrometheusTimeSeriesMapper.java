package com.amazonaws.examples.flink.map;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

public class AggregateEventsToPrometheusTimeSeriesMapper implements MapFunction<AggregateVehicleEvent, PrometheusTimeSeries> {
    @Override
    public PrometheusTimeSeries map(AggregateVehicleEvent aggregateEvent) {
        return PrometheusTimeSeries.builder()
                .withMetricName(getMetricName(aggregateEvent))
                .addLabel("model", aggregateEvent.getVehicleModel())
                .addLabel("region", aggregateEvent.getRegion())
                .addSample(aggregateEvent.getCount(), aggregateEvent.getTimestamp())
                .build();
    }

    private static String getMetricName(AggregateVehicleEvent aggregateEvent) {
        return aggregateEvent.getEventType().name().toLowerCase();
    }
}
