package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.types.PojoTestUtils;

import com.amazonaws.examples.flink.FlinkSerializationTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
class VehiclesInMotionAggregateFunctionTest {

    @Test
    void shouldSerializeDeserializeWithoutKryo() throws IOException {
        Map<String, Boolean> vehiclesInMotion = new HashMap<>();
        vehiclesInMotion.put("V001", true);
        vehiclesInMotion.put("V002", true);
        VehiclesInMotionAggregateFunction.Accumulator accumulator =  new VehiclesInMotionAggregateFunction.Accumulator(42L, "US", "Model-T", vehiclesInMotion);

        VehiclesInMotionAggregateFunction.Accumulator deserialized = FlinkSerializationTestUtils.serializeDeserializeNoKryo(accumulator, VehiclesInMotionAggregateFunction.Accumulator.class);

        assertEquals(accumulator.getLastTimestamp(), deserialized.getLastTimestamp());
        assertEquals(accumulator.getVehicleModel(), deserialized.getVehicleModel());
        assertEquals(accumulator.getRegion(), deserialized.getRegion());
        assertEquals(accumulator.getVehiclesInMotion(), deserialized.getVehiclesInMotion());
    }
}