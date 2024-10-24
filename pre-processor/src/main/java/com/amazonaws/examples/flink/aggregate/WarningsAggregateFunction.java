package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

public class WarningsAggregateFunction implements AggregateFunction<EnrichedVehicleEvent, WarningsAggregateFunction.Accumulator, AggregateVehicleEvent> {

    @AllArgsConstructor
    @NoArgsConstructor
    public static class Accumulator {
        private long lastTimestamp;
        private String region;
        private String vehicleModel;
        private int warningCount;
        private Set<String> assessedVehicles = new HashSet<>();
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(EnrichedVehicleEvent event, Accumulator accumulator) {
        Preconditions.checkArgument(accumulator.vehicleModel == null || accumulator.vehicleModel.equals(event.getVehicleModel()), "Cannot accumulate different vehicle models");
        Preconditions.checkArgument(accumulator.region == null || accumulator.region.equals(event.getRegion()), "Cannot accumulate different regions");
        accumulator.region = event.getRegion();
        accumulator.vehicleModel = event.getVehicleModel();

        if( event.getEventType() == EventType.WARNINGS ) {
            accumulator.lastTimestamp = Math.max(accumulator.lastTimestamp, event.getTimestamp());

            String vehicleId = event.getVehicleId();
            // If this vehicle has not been assessed already, add its warning count
            if(!accumulator.assessedVehicles.contains(vehicleId)) {
                accumulator.warningCount += event.getMeasurement();
                accumulator.assessedVehicles.add(event.getVehicleId());
            }
            return accumulator;
        }
        // Ignore other types of events

        return accumulator;
    }

    @Override
    public AggregateVehicleEvent getResult(Accumulator accumulator) {
        return AggregateVehicleEvent.newWarningsEvent(accumulator.vehicleModel, accumulator.region, accumulator.lastTimestamp, accumulator.warningCount);
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Preconditions.checkArgument(a.vehicleModel == null || a.vehicleModel.equals(b.vehicleModel), "Cannot merge different vehicle models");
        Preconditions.checkArgument(a.region == null || a.region.equals(b.region), "Cannot merge different regions");

        // Merge the two accumulators by summing the warning counts and taking the maximum timestamp
        a.assessedVehicles.addAll(b.assessedVehicles);
        return new Accumulator(Math.max(a.lastTimestamp, b.lastTimestamp), b.region, b.vehicleModel, a.warningCount + b.warningCount, a.assessedVehicles);
    }
}
