package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.types.PojoTestUtils;

import org.junit.jupiter.api.Test;

class WarningsAggregateFunctionTest {

    // Verify serialization does not fall back to Kryo
    @Test
    void testAccumulatorSerialization() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(WarningsAggregateFunction.Accumulator.class);
    }

}