package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of vehicles in motion aggregation based on ProcessWindowFunction.
 * <p>
 * PROs: Lightweight state serialization. State is composed by EnrichedVehicleEvent.
 * <p>
 * CONs: Bigger state. All events in the window are stored in state
 * <p>
 * Note: based on tests, this implementation is more efficient than {@link VehiclesInMotionAggregateFunction}
 */
public class VehiclesInMotionProcessWindowFunction extends ProcessWindowFunction<EnrichedVehicleEvent, AggregateVehicleEvent, String, TimeWindow> {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(VehiclesInMotionProcessWindowFunction.class);

    @Override
    public void process(String key, ProcessWindowFunction<EnrichedVehicleEvent, AggregateVehicleEvent, String, TimeWindow>.Context context, Iterable<EnrichedVehicleEvent> elements, Collector<AggregateVehicleEvent> out) throws Exception {
        Set<String> vehiclesInMotion = new HashSet<>();
        long timestamp = 0L;
        String model = null;
        String region = null;

        boolean first = true;
        for (EnrichedVehicleEvent event : elements) {
            EventType eventType = event.getEventType();
            if (eventType == EventType.IC_RPM || eventType == EventType.ELECTRIC_RPM) {
                // Capture model and region from the first element
                if (first) {
                    model = event.getVehicleModel();
                    region = event.getRegion();
                    first = false;
                }
                // Max timestamp among all events
                timestamp = Math.max(timestamp, event.getTimestamp());

                // If any motor is revolving, consider the vehicle in motion
                if (event.getMeasurement() != 0) {
                    vehiclesInMotion.add(event.getVehicleId());
                }
            }
        }

        // Emit aggregate record only if actual events have been collected
        if (timestamp > 0L) {
            int vehicleInMotionCount = vehiclesInMotion.size();
            AggregateVehicleEvent aggregate = AggregateVehicleEvent.newVehiclesInMotionEvent(model, region, timestamp, vehicleInMotionCount);
            LOGGER.trace("Aggregate result: {}", aggregate);
            out.collect(aggregate);
        }
    }
}
