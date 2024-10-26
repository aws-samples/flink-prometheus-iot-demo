package com.amazonaws.examples.flink.filter;

import org.apache.flink.api.common.functions.FilterFunction;

import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;

import java.util.List;

/**
 * Filter function to include only specified event types
 */
public class IncludeEventTypes implements FilterFunction<EnrichedVehicleEvent> {
    private final List<EventType> eventTypes;

    public IncludeEventTypes(List<EventType> eventTypes) {
        this.eventTypes = eventTypes;
    }

    public IncludeEventTypes(EventType... eventTypes) {
        this(List.of(eventTypes));
    }

    @Override
    public boolean filter(EnrichedVehicleEvent event) {
        return eventTypes.contains(event.getEventType());
    }
}
