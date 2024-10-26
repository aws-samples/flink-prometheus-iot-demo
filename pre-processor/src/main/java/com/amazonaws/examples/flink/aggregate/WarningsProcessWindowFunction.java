package com.amazonaws.examples.flink.aggregate;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import org.slf4j.Logger;

import java.util.Set;

/**
 * Implementation of warning aggregation based on ProcessWindowFunction.
 * <p>
 * PROs: Lightweight state serialization. State is composed by EnrichedVehicleEvent.
 * <p>
 * CONs: Bigger state. All events in the window are stored in state.
 * <p>
 * Note: based on tests, this implementation is more efficient than {@link WarningsAggregateFunction}
 */
public class WarningsProcessWindowFunction extends ProcessWindowFunction<EnrichedVehicleEvent, AggregateVehicleEvent, String, TimeWindow> {
    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(WarningsProcessWindowFunction.class);

    @Override
    public void process(String key, ProcessWindowFunction<EnrichedVehicleEvent, AggregateVehicleEvent, String, TimeWindow>.Context context, Iterable<EnrichedVehicleEvent> elements, Collector<AggregateVehicleEvent> out) throws Exception {
        Set<String> assessedVehicles = new java.util.HashSet<>();
        long timestamp = 0L;
        String model = null;
        String region = null;
        int warningCount = 0;

        boolean first = true;
        for (EnrichedVehicleEvent event : elements) {
            EventType eventType = event.getEventType();
            if (eventType == EventType.WARNINGS) {
                // Capture model and region from the first element
                if (first) {
                    model = event.getVehicleModel();
                    region = event.getRegion();
                    first = false;
                }
                // Max timestamp among all events
                timestamp = Math.max(timestamp, event.getTimestamp());

                // Only count warning once for each vehicleId
                String vehicleId = event.getVehicleId();
                if (!assessedVehicles.contains(vehicleId)) {
                    warningCount += event.getMeasurement();
                    assessedVehicles.add(event.getVehicleId());
                }
            }
        }

        // Emit aggregate record only if actual events have been collected
        if (timestamp > 0L) {
            AggregateVehicleEvent aggregate = AggregateVehicleEvent.newWarningsEvent(model, region, timestamp, warningCount);
            LOGGER.trace("Aggregate result: {}", aggregate);
            out.collect(aggregate);
        }
    }
}
