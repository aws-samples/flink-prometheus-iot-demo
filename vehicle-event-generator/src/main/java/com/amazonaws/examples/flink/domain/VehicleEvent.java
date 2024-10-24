package com.amazonaws.examples.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor @Builder
public class VehicleEvent {

    @NonNull
    private EventType eventType;
    @NonNull
    private String vehicleId;
    private long timestamp;
    private int measurement;
    private String region;
}
