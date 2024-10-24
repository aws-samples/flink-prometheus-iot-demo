package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

public class VehiclesInMotionAggregateFunction implements AggregateFunction<EnrichedVehicleEvent, VehiclesInMotionAggregateFunction.Accumulator, AggregateVehicleEvent> {

    @AllArgsConstructor
    @NoArgsConstructor
    public static class Accumulator {
        private long lastTimestamp;
        private String region;
        private String vehicleModel;
        private Set<String> vehiclesInMotion = new HashSet<>();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(EnrichedVehicleEvent event, Accumulator accumulator) {
        Preconditions.checkArgument(accumulator.vehicleModel == null || accumulator.vehicleModel.equals(event.getVehicleModel()), "Cannot accumulate different vehicle models");
        Preconditions.checkArgument(accumulator.region == null || accumulator.region.equals(event.getRegion()), "Cannot accumulate different regions");

        accumulator.vehicleModel = event.getVehicleModel();
        accumulator.region = event.getRegion();
        switch (event.getEventType()) {
            case IC_RPM:
            case ELECTRIC_RPM:
                if (event.getMeasurement() != 0) {
                    // The vehicle is in motion
                    String vehicleId = event.getVehicleId();
                    accumulator.vehiclesInMotion.add(vehicleId);

                }
                accumulator.lastTimestamp = event.getTimestamp();
                return accumulator;
            default:
                // Any other event does not change the accumulator
                return accumulator;
        }
    }

    @Override
    public AggregateVehicleEvent getResult(Accumulator accumulator) {
        int vehicleInMotionCount = accumulator.vehiclesInMotion.size();
        return AggregateVehicleEvent.newVehiclesInMotionEvent(accumulator.vehicleModel, accumulator.region, accumulator.lastTimestamp, vehicleInMotionCount);
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Preconditions.checkArgument(a.vehicleModel == null || a.vehicleModel.equals(b.vehicleModel), "Cannot merge different vehicle models");
        Preconditions.checkArgument(a.region == null || a.region.equals(b.region), "Cannot merge different regions");

        // Merge the two sets of vehicles in motion
        a.vehiclesInMotion.addAll(b.vehiclesInMotion);
        return new Accumulator(Math.max(a.lastTimestamp, b.lastTimestamp), b.region, b.vehicleModel, a.vehiclesInMotion);
    }
}
