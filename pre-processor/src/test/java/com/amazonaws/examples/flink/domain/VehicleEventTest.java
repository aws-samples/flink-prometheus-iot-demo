package com.amazonaws.examples.flink.domain;

import com.amazonaws.examples.flink.FlinkSerializationTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VehicleEventTest {

    @Test
    void shouldSerializeDeserializeWithoutKryo() throws IOException {
        VehicleEvent record = new VehicleEvent(EventType.IC_RPM, "V001", 42L, 13, "US");

        VehicleEvent deserializedRecord = FlinkSerializationTestUtils.serializeDeserializeNoKryo(record, VehicleEvent.class);

        assertEquals(record.getEventType(), deserializedRecord.getEventType());
        assertEquals(record.getVehicleId(), deserializedRecord.getVehicleId());
        assertEquals(record.getTimestamp(), deserializedRecord.getTimestamp());
        assertEquals(record.getRegion(), deserializedRecord.getRegion());
    }
}