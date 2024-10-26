package com.amazonaws.examples.flink.domain;

import org.apache.flink.types.PojoTestUtils;

import org.junit.jupiter.api.Test;

class EnrichedVehicleEventTest {

    // Verify serialization does not fall back to Kryo
    @Test
    void testSerialization() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(EnrichedVehicleEvent.class);
    }

}