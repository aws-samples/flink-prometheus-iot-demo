package com.amazonaws.examples.flink.datagen;

import com.amazonaws.examples.flink.domain.VehicleEvent;
import org.junit.jupiter.api.RepeatedTest;

import static com.amazonaws.examples.flink.datagen.VehicleEventGeneratorFunction.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VehicleEventGeneratorFunctionTest {

    private static final ParallelGeneratorFunction.KeyIndexSplit KEY_INDEX_SPLIT = new ParallelGeneratorFunction.KeyIndexSplit(0, 1);

    // This test is not deterministic, but we just want to exercise the generator
    @RepeatedTest(value = 100, name = "Iteration {currentRepetition}/{totalRepetitions}")
    void generatedMeasurementsShouldBeInRange() {
        VehicleEventGenerator generator = new VehicleEventGenerator();

        VehicleEvent vehicleEvent = generator.apply(0L, KEY_INDEX_SPLIT);

        switch (vehicleEvent.getEventType()) {
            case IC_RPM:
                assertTrue(vehicleEvent.getMeasurement() >= MIN_IC_RPM && vehicleEvent.getMeasurement() < MAX_IC_RPM);
                break;
            case ELECTRIC_RPM:
                assertTrue(vehicleEvent.getMeasurement() >= MIN_ELECTRIC_RPM && vehicleEvent.getMeasurement() < MAX_ELECTRIC_RPM);
                break;
            case WARNINGS:
                assertTrue(vehicleEvent.getMeasurement() >= 0 && vehicleEvent.getMeasurement() < MAX_WARNINGS);
        }
    }
}