package com.amazonaws.examples.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class VehicleEvent {
    private EventType eventType;
    private String vehicleId;
    private long timestamp;
    private int measurement;
    private String region;
}
