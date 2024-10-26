package com.amazonaws.examples.flink.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

import com.amazonaws.examples.flink.domain.VehicleEvent;

public class VehicleEventToPrometheusTimeSeriesMapper implements MapFunction<VehicleEvent, PrometheusTimeSeries> {
    @Override
    public PrometheusTimeSeries map(VehicleEvent event) throws Exception {
        return PrometheusTimeSeries.builder()
                .withMetricName(event.getEventType().name().toLowerCase())
                .addLabel("vehicleId", event.getVehicleId())
                .addLabel("region", event.getRegion())
                .addSample(event.getMeasurement(), event.getTimestamp())
                .build();
    }
}
