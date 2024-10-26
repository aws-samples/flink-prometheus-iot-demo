package com.amazonaws.examples.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class VehicleEvent {
    private EventType eventType;
    private String vehicleId;
    private long timestamp;
    private int measurement;
    private String region;
}
