package com.amazonaws.examples.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data @NoArgsConstructor
@AllArgsConstructor
public class AggregateVehicleEvent {

    public enum AggregateEventType {
        VEHICLES_IN_MOTION,
        WARNINGS
    }

    @NonNull
    private AggregateEventType eventType;
    @NonNull
    private String vehicleModel;
    @NonNull
    private String region;
    private long timestamp;
    private int count;

    public static AggregateVehicleEvent newVehiclesInMotionEvent(String vehicleModel, String region, long timestamp, int count) {
        return new AggregateVehicleEvent(AggregateEventType.VEHICLES_IN_MOTION, vehicleModel, region, timestamp, count);
    }

    public static AggregateVehicleEvent newWarningsEvent(String vehicleModel, String region, long timestamp, int count) {
        return new AggregateVehicleEvent(AggregateEventType.WARNINGS, vehicleModel, region, timestamp, count);
    }
}
