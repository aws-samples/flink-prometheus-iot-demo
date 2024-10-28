package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.types.PojoTestUtils;

import com.amazonaws.examples.flink.FlinkSerializationTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WarningsAggregateFunctionTest {

    @Test
    void shouldSerializeDeserializeWithoutKryo() throws IOException {
        Map<String, Boolean> assessedVehicles = new HashMap<>();
        assessedVehicles.put("V001", true);
        assessedVehicles.put("V002", true);

        WarningsAggregateFunction.Accumulator accumulator = new WarningsAggregateFunction.Accumulator(42L, "US", "Model-T", 1234 ,assessedVehicles);

        WarningsAggregateFunction.Accumulator deserialized = FlinkSerializationTestUtils.serializeDeserializeNoKryo(accumulator, WarningsAggregateFunction.Accumulator.class);

        assertEquals(accumulator.getLastTimestamp(), deserialized.getLastTimestamp());
        assertEquals(accumulator.getVehicleModel(), deserialized.getVehicleModel());
        assertEquals(accumulator.getRegion(), deserialized.getRegion());
        assertEquals(accumulator.getWarningCount(), deserialized.getWarningCount());
        assertEquals(accumulator.getAssessedVehicles(), deserialized.getAssessedVehicles());
    }
}