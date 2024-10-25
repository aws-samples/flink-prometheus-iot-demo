package com.amazonaws.examples.flink.enrich;

import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import com.amazonaws.examples.flink.domain.VehicleEvent;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// Simplistic text to exercise the enrichment function
class VehicleModelEnrichmentFunctionTest {

    private final VehicleModelEnrichmentFunction vehicleModelEnrichmentFunction = new VehicleModelEnrichmentFunction();


    @ParameterizedTest
    @ValueSource(strings = {"V0000000", "V0001092"})
    void shouldReturnTypeA(String vehicleId) throws Exception {
        VehicleEvent vehicleEvent = VehicleEvent.builder()
                .vehicleId(vehicleId)
                .region("EMEA")
                .eventType(EventType.IC_RPM)
                .measurement(42)
                .timestamp(0L)
                .build();

        EnrichedVehicleEvent enrichedVehicleEvent = vehicleModelEnrichmentFunction.map(vehicleEvent);

        assertEquals("Type-A", enrichedVehicleEvent.getVehicleModel());
    }


    @ParameterizedTest
    @ValueSource(strings = {"V0000025", "V0001117"})
    void shouldReturnTypeZ(String vehicleId) throws Exception {
        VehicleEvent vehicleEvent = VehicleEvent.builder()
                .vehicleId(vehicleId)
                .region("EMEA")
                .eventType(EventType.IC_RPM)
                .measurement(42)
                .timestamp(0L)
                .build();

        EnrichedVehicleEvent enrichedVehicleEvent = vehicleModelEnrichmentFunction.map(vehicleEvent);

        assertEquals("Type-Z", enrichedVehicleEvent.getVehicleModel());
    }
}