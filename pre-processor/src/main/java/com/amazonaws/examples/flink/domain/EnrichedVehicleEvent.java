package com.amazonaws.examples.flink.domain;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

/**
 * Enriched vehicle event with additional vehicle model field
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@NoArgsConstructor
@ToString(callSuper = true)
public class EnrichedVehicleEvent extends VehicleEvent {
    private String vehicleModel;

    public EnrichedVehicleEvent(@NonNull EventType eventType, @NonNull String vehicleId, long timestamp, int measurement, String region, String vehicleModel) {
        super(eventType, vehicleId, timestamp, measurement, region);
        this.vehicleModel = vehicleModel;
    }

    public static EnrichedVehicleEvent copyFrom(VehicleEvent vehicleEvent, String vehicleModel) {
        return new EnrichedVehicleEvent(
                vehicleEvent.getEventType(),
                vehicleEvent.getVehicleId(),
                vehicleEvent.getTimestamp(),
                vehicleEvent.getMeasurement(),
                vehicleEvent.getRegion(),
                vehicleModel);
    }

    public static EnrichedVehicleEvent copyWithNewMeasurement(EnrichedVehicleEvent vehicleEvent, int newMeasurement) {
        return new EnrichedVehicleEvent(
                vehicleEvent.getEventType(),
                vehicleEvent.getVehicleId(),
                vehicleEvent.getTimestamp(),
                newMeasurement,
                vehicleEvent.getRegion(),
                vehicleEvent.getVehicleModel());
    }
}
