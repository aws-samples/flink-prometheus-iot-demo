package com.amazonaws.examples.flink.enrich;

import org.apache.flink.api.common.functions.MapFunction;

import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.VehicleEvent;

/**
 * This function simulates an enrichment step: each VehicleEvent is enriched adding the model.
 * For simplicity, we derive the model based on the vehicleId with a simple formula.
 * <p>
 * In a real application this would happen looking up to some reference data, using an external endpoint or a database,
 * and the enrichment should use Flink AsyncIO, cache vehicle metadata, or leverage other patterns to avoid calling an
 * external resource synchronously, and on every event.
 */
public class VehicleModelEnrichmentFunction implements MapFunction<VehicleEvent, EnrichedVehicleEvent> {

    private static final int MIN_MODEL = 'A';
    private static final int MAX_MODEL = 'Z';
    private static final int NUM_MODELS = MAX_MODEL - MIN_MODEL + 1;

    private static String vehicleModel(String vehicleId) {
        int vehicleIndex = Integer.parseInt(vehicleId.substring(1));
        char modelChar = (char) (MIN_MODEL + vehicleIndex % NUM_MODELS);
        return "Type-" + modelChar;
    }


    @Override
    public EnrichedVehicleEvent map(VehicleEvent vehicleEvent) throws Exception {
        String model = vehicleModel(vehicleEvent.getVehicleId());
        return EnrichedVehicleEvent.copyFrom(vehicleEvent, model);
    }
}
