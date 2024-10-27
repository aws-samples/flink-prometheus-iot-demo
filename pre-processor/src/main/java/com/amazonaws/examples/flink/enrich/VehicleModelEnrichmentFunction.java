package com.amazonaws.examples.flink.enrich;

import org.apache.flink.api.common.functions.MapFunction;

import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.VehicleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function simulates an enrichment step: each VehicleEvent is enriched adding the model.
 * For simplicity, we derive the model based on the vehicleId.
 * <p>
 * In a real application this would happen looking up to some reference data, using an external endpoint or a database,
 * and the enrichment should use Flink AsyncIO, cache vehicle metadata, or leverage other patterns to avoid calling an
 * external resource synchronously, and on every event.
 */
public class VehicleModelEnrichmentFunction implements MapFunction<VehicleEvent, EnrichedVehicleEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(VehicleModelEnrichmentFunction.class);

    private static final String[] MODELS = {
            "Astralis",
            "Nebulara",
            "Galantor",
            "Equinoxia",
            "Orbitra",
            "Nebulara",
            "Polara",
            "Helion"
    };

    private static String vehicleModel(String vehicleId) {
        int vehicleIndex = Integer.parseInt(vehicleId.substring(1));
        return MODELS[vehicleIndex % MODELS.length];
    }


    @Override
    public EnrichedVehicleEvent map(VehicleEvent vehicleEvent) throws Exception {
        String model = vehicleModel(vehicleEvent.getVehicleId());
        return EnrichedVehicleEvent.copyFrom(vehicleEvent, model);
    }
}
