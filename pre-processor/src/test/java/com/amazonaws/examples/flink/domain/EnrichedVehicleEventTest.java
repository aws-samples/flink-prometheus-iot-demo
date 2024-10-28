package com.amazonaws.examples.flink.domain;

import com.amazonaws.examples.flink.FlinkSerializationTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EnrichedVehicleEventTest {


    @Test
    void shouldSerializeDeserializeWithoutKryo() throws IOException {
        EnrichedVehicleEvent record = new EnrichedVehicleEvent(EventType.IC_RPM, "V001", 42L, 13, "US", "Model-T");

        EnrichedVehicleEvent deserializedRecord = FlinkSerializationTestUtils.serializeDeserializeNoKryo(record, EnrichedVehicleEvent.class);

        assertEquals(record.getEventType(), deserializedRecord.getEventType());
        assertEquals(record.getVehicleId(), deserializedRecord.getVehicleId());
        assertEquals(record.getTimestamp(), deserializedRecord.getTimestamp());
        assertEquals(record.getRegion(), deserializedRecord.getRegion());
        assertEquals(record.getVehicleModel(), deserializedRecord.getVehicleModel());
    }
}