package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Preconditions;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Implementation of vehicles in motion aggregation based on AggregateFunction.
 * <p>
 * PROs: Smaller state. Aggregates the results on each event.
 * <p>
 * CONs: State serialization overhead. The accumulator contains a Map. This is heavy to serialize/deserialize,
 * regardless preventing fallback to Kryo defining a custom TypeInfoFactory
 */
public class VehiclesInMotionAggregateFunction implements AggregateFunction<EnrichedVehicleEvent, VehiclesInMotionAggregateFunction.Accumulator, AggregateVehicleEvent> {

    @AllArgsConstructor
    @NoArgsConstructor
    @Getter
    @Setter
    @ToString
    @TypeInfo(AccumulatorTypeInfoFactory.class)
    public static class Accumulator {
        private long lastTimestamp;
        private String region;
        private String vehicleModel;
        private Map<String, Boolean> vehiclesInMotion = new HashMap<>(); // Use a Map of boolean instead of a Set, because Flink does not have serializers for Sets
    }

    /**
     * TypeInfoFactory to serialize the Accumulator class without falling back to Kryo
     */
    public static class AccumulatorTypeInfoFactory extends TypeInfoFactory<Accumulator> {
        @Override
        public TypeInformation<Accumulator> createTypeInfo(Type type, Map<String, TypeInformation<?>> genericParameters) {
            return Types.POJO(
                    Accumulator.class,
                    Map.of(
                            "lastTimestamp", Types.LONG,
                            "region", Types.STRING,
                            "vehicleModel", Types.STRING,
                            "vehiclesInMotion", Types.MAP(Types.STRING, Types.BOOLEAN)
                    ));
        }
    }


    @Override
    public Accumulator createAccumulator() {
        // Note: the accumulator is created with null region and model
        return new Accumulator();
    }

    @Override
    public Accumulator add(EnrichedVehicleEvent event, Accumulator accumulator) {
        Preconditions.checkArgument(accumulator.vehicleModel == null || accumulator.vehicleModel.equals(event.getVehicleModel()), "Cannot accumulate different vehicle models");
        Preconditions.checkArgument(accumulator.region == null || accumulator.region.equals(event.getRegion()), "Cannot accumulate different regions");
        Preconditions.checkArgument(event.getEventType() == EventType.IC_RPM || event.getEventType() == EventType.ELECTRIC_RPM, "Cannot accumulate events that are not motor RPM");
        // Replace model and region in the accumulator with values from the event
        accumulator.vehicleModel = event.getVehicleModel();
        accumulator.region = event.getRegion();
        accumulator.lastTimestamp = Math.max(accumulator.lastTimestamp, event.getTimestamp());

        if (event.getMeasurement() != 0) {
            // The vehicle is in motion
            String vehicleId = event.getVehicleId();
            accumulator.vehiclesInMotion.put(vehicleId, true);
        }

        return accumulator;
    }

    @Override
    public AggregateVehicleEvent getResult(Accumulator accumulator) {
        int vehicleInMotionCount = accumulator.vehiclesInMotion.size();
        return AggregateVehicleEvent.newVehiclesInMotionEvent(accumulator.vehicleModel, accumulator.region, accumulator.lastTimestamp, vehicleInMotionCount);
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Preconditions.checkArgument(a.vehicleModel == null || b.vehicleModel == null || a.vehicleModel.equals(b.vehicleModel), "Cannot merge different vehicle models");
        Preconditions.checkArgument(a.region == null || b.region == null || a.region.equals(b.region), "Cannot merge different regions");

        // Merge the two sets of vehicles in motion
        String model = Optional.ofNullable(a.vehicleModel).orElse(b.vehicleModel);
        String region = Optional.ofNullable(a.region).orElse(b.region);
        long timestamp = Math.max(a.lastTimestamp, b.lastTimestamp);
        Map<String, Boolean> vehiclesInMotion = new HashMap<>();
        vehiclesInMotion.putAll(a.vehiclesInMotion);
        vehiclesInMotion.putAll(b.vehiclesInMotion);

        a.vehiclesInMotion.putAll(b.vehiclesInMotion);
        return new Accumulator(timestamp, region, model, vehiclesInMotion);
    }

}
