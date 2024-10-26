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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class WarningsAggregateFunction implements AggregateFunction<EnrichedVehicleEvent, WarningsAggregateFunction.Accumulator, AggregateVehicleEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(WarningsAggregateFunction.class);

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
        private int warningCount = 0;
        private Map<String, Boolean> assessedVehicles = new HashMap<>(); // Use a Map of boolean instead of a Set, because Flink does not have serializers for Sets
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
                            "warningCount", Types.INT,
                            "assessedVehicles", Types.MAP(Types.STRING, Types.BOOLEAN)
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
        Preconditions.checkArgument(event.getEventType() == EventType.WARNINGS, "Cannot accumulate non-warning events");


        // Replace model and region in the accumulator with values from the event
        accumulator.region = event.getRegion();
        accumulator.vehicleModel = event.getVehicleModel();
        accumulator.lastTimestamp = Math.max(accumulator.lastTimestamp, event.getTimestamp());

        String vehicleId = event.getVehicleId();
        // If this vehicle has not been assessed already, add its warning count
        if (!accumulator.assessedVehicles.containsKey(vehicleId)) {
            accumulator.warningCount += event.getMeasurement();
            accumulator.assessedVehicles.put(event.getVehicleId(), true);
        }

        return accumulator;
    }

    @Override
    public AggregateVehicleEvent getResult(Accumulator accumulator) {
        return AggregateVehicleEvent.newWarningsEvent(
                accumulator.vehicleModel,
                accumulator.region,
                accumulator.lastTimestamp,
                accumulator.warningCount);
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Preconditions.checkArgument(a.vehicleModel == null || b.vehicleModel == null || a.vehicleModel.equals(b.vehicleModel), "Cannot merge different vehicle models");
        Preconditions.checkArgument(a.region == null || b.region == null || a.region.equals(b.region), "Cannot merge different regions");

        // Merge the two accumulators by summing the warning counts and taking the maximum timestamp
        String model = Optional.ofNullable(a.vehicleModel).orElse(b.vehicleModel);
        String region = Optional.ofNullable(a.region).orElse(b.region);
        long timestamp = Math.max(a.lastTimestamp, b.lastTimestamp);
        int count = a.warningCount + b.warningCount;
        Map<String, Boolean> assessedVehicles = new HashMap<>();
        assessedVehicles.putAll(a.assessedVehicles);
        assessedVehicles.putAll(b.assessedVehicles);

        return new Accumulator(timestamp, region, model, count, assessedVehicles);
    }
}
