package com.amazonaws.examples.flink.datagen;

import org.apache.flink.annotation.VisibleForTesting;

import com.amazonaws.examples.flink.domain.VehicleEvent;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static com.amazonaws.examples.flink.domain.VehicleEvent.EventType.EVENT_TYPES;
import static java.util.Map.entry;

/**
 * Parallel generator function that generates semi-random VehicleEvents.
 * <p>
 * Previously generated events for that vehicle are retained in memory, and new generated events are random variations of the previous value,
 * between fixed min and max values.
 */
public class VehicleEventGeneratorFunction extends ParallelGeneratorFunction<VehicleEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(VehicleEventGeneratorFunction.class);

    public static final int MAX_IC_RPM = 3000;
    public static final int MIN_IC_RPM = 0;
    public static final int MAX_ELECTRIC_RPM = 10000;
    public static final int MIN_ELECTRIC_RPM = -10000;
    public static final int MAX_WARNINGS = 5;

    private static final double MAX_RPM_VARIATION_PER_EVENT = 20.0 / 100;
    private static final double CHANCES_WARNING_TURNING_OFF = 1.0 / 100;
    private static final double CHANCES_WARNING_TURNING_ON = 1.0 / 100;

    private static String vehicleId(int vehicleIndex) {
        return String.format("V%010d", vehicleIndex);
    }

    private static int randomMotorRpm(int prevRpm, int minRpm, int maxRpm) {
        int randomVariation = (int) ((RandomUtils.nextDouble(0, MAX_RPM_VARIATION_PER_EVENT * 2) - MAX_RPM_VARIATION_PER_EVENT) * (maxRpm - minRpm));
        return Math.max(minRpm, Math.min(maxRpm, prevRpm + randomVariation));
    }

    private static int randomWarnings(int prevWarnings) {
        // Warning lights turn on/off < 1% of the times, up to MAX_WARNINGS  turned on per vehicle
        double dice = RandomUtils.nextDouble(0, 1);
        if (dice < CHANCES_WARNING_TURNING_OFF) {
            return Math.max(0, prevWarnings - 1);
        } else if (dice > 1 - CHANCES_WARNING_TURNING_ON) {
            return Math.min(MAX_WARNINGS, prevWarnings + 1);
        } else {
            return prevWarnings;
        }
    }

    @VisibleForTesting
    static class VehicleEventGenerator implements BiFunction<Long, KeyIndexSplit, VehicleEvent>, Serializable {
        private final Map<Integer, Map<VehicleEvent.EventType, Integer>> lastMeasurements = new HashMap<>();

        /**
         * Generates a semi-random new measurement, for a specific vehicle index and event type, considering the previous value
         * of that measurement for the same vehicle and event type.
         * The goal is to make more or less realistic the generated events, that will randomly oscillate between min
         * and max values, as opposed to jumping to a random value at every new event
         */
        private Integer nextMeasurement(int vehicleIndex, VehicleEvent.EventType eventType) {
            // Use the previous value as base value
            Map<VehicleEvent.EventType, Integer> lastVehicleMeasurements = lastMeasurements.getOrDefault(
                    vehicleIndex,
                    new HashMap<>(
                            Map.ofEntries(
                                    entry(VehicleEvent.EventType.IC_RPM, 0),
                                    entry(VehicleEvent.EventType.ELECTRIC_RPM, 0),
                                    entry(VehicleEvent.EventType.WARNINGS, 0)))
            );
            int lastMeasurement = lastVehicleMeasurements.get(eventType);

            int newMeasurement;
            switch (eventType) {
                case IC_RPM:
                    newMeasurement = randomMotorRpm(lastMeasurement, MIN_IC_RPM, MAX_IC_RPM);
                    break;
                case ELECTRIC_RPM:
                    newMeasurement = randomMotorRpm(lastMeasurement, MIN_ELECTRIC_RPM, MAX_ELECTRIC_RPM);
                    break;
                case WARNINGS:
                    newMeasurement = randomWarnings(lastMeasurement);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown event type " + eventType);
            }
            // Update last measurements for the next generated event
            lastVehicleMeasurements.put(eventType, newMeasurement);
            lastMeasurements.put(vehicleIndex, lastVehicleMeasurements);

            return newMeasurement;
        }

        private VehicleEvent nextEvent(int vehicleIndex) {
            // Vehicle ID
            String vehicleId = vehicleId(vehicleIndex);

            // Event timestamp
            long eventTimestamp = System.currentTimeMillis();

            // Random event type
            VehicleEvent.EventType eventType = EVENT_TYPES[RandomUtils.nextInt(0, EVENT_TYPES.length)];

            // Semi-Random new measurement
            int measurement = nextMeasurement(vehicleIndex, eventType);

            return new VehicleEvent(eventType, vehicleId, eventTimestamp, measurement);
        }

        @Override
        public VehicleEvent apply(Long sourceInputValue, KeyIndexSplit keyIndexSplit) {
            // Source input value is not used

            // Generates a random vehicle index in the key split
            int vehicleIndex = RandomUtils.nextInt(keyIndexSplit.getStartKeyIndex(), keyIndexSplit.getEndKeyIndex());

            // Generates a semi-random event for this vehicle
            VehicleEvent vehicleEvent = nextEvent(vehicleIndex);

            LOG.trace("Next event: {}", vehicleEvent);
            return vehicleEvent;
        }

    }

    public VehicleEventGeneratorFunction(int nrOfVehicles) {
        super(new VehicleEventGenerator(), 0, nrOfVehicles);
    }
}
