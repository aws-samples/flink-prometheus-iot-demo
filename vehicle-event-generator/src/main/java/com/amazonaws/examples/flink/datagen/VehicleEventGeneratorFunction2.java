package com.amazonaws.examples.flink.datagen;

import org.apache.flink.annotation.VisibleForTesting;

import com.amazonaws.examples.flink.domain.EventType;
import com.amazonaws.examples.flink.domain.VehicleEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static com.amazonaws.examples.flink.domain.EventType.WARNINGS;

/**
 * Parallel generator function that generates semi-random VehicleEvents.
 * Keeps track of the vehicle in motions and the number of warnings.
 * Change the in-motion state or the warnings, based on probabilities.
 */
public class VehicleEventGeneratorFunction2 extends ParallelGeneratorFunction<VehicleEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(VehicleEventGeneratorFunction2.class);

    public static final String[] REGIONS = {
            "US", "US", "US", "US", "US", "US", "US", "US", "US",
            "Canada",
            "Mexico",
            "UK", "UK", "UK",
            "Germany", "Germany", "Germany", "Germany",
            "France", "France",
            "Italy",
            "Spain",
            "Poland",
            "Sweden", "Sweden",
            "Norway",
            "Australia",
            "New Zealand",
            "Japan", "Japan", "Japan",
            "Thailand",
            "Malaysia",
            "South Africa",
            "Brazil",
            "Argentina",
            "UAE"
    };

    public static final int MAX_IC_RPM = 3000;
    public static final int MIN_IC_RPM = 0;
    public static final int MAX_ELECTRIC_RPM = 10000;
    public static final int MIN_ELECTRIC_RPM = -10000;
    public static final int MAX_WARNINGS = 5;

    private static String vehicleId(int vehicleIndex) {
        return String.format("V%010d", vehicleIndex);
    }

    public VehicleEventGeneratorFunction2(int nrOfVehicles, GeneratorConfig config) {
        super(new VehicleEventGenerator(config), 0, nrOfVehicles);
    }

    @Getter
    @Builder
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class GeneratorConfig implements Serializable {
        private double probabilityMotionStateChange;
        private double probabilityWarningChange;
    }

    // Deterministically decide the Region of each vehicle
    private static String getVehicleRegion(int vehicleIndex) {
        return REGIONS[vehicleIndex % REGIONS.length];
    }

    private static double rollDice() {
        return RandomUtils.nextDouble(0, 1);
    }


    @VisibleForTesting
    static class VehicleEventGenerator implements BiFunction<Long, KeyIndexSplit, VehicleEvent>, Serializable {
        private final Map<Integer, Boolean> vehicleInMotion = new HashMap<>();
        private final Map<Integer, Integer> warnings = new HashMap<>();
        private final GeneratorConfig config;

        public VehicleEventGenerator(GeneratorConfig config) {
            this.config = config;
        }

        private boolean isInMotion(int vehicleIndex) {
            if (!vehicleInMotion.containsKey(vehicleIndex)) {
                vehicleInMotion.put(vehicleIndex, false);
            }
            return vehicleInMotion.get(vehicleIndex);
        }

        private int getWarnings(int vehicleIndex) {
            if (!warnings.containsKey(vehicleIndex)) {
                warnings.put(vehicleIndex, 0);
            }
            return warnings.get(vehicleIndex);
        }


        private static VehicleEvent randomMotorEvent(String vehicleId, long eventTimestamp, String region) {
            EventType eventType = (rollDice() < 0.5) ? EventType.IC_RPM : EventType.ELECTRIC_RPM;
            int rpm = (eventType == EventType.IC_RPM)
                    ? RandomUtils.nextInt(MIN_IC_RPM, MAX_IC_RPM)
                    : RandomUtils.nextInt(0, MAX_ELECTRIC_RPM - MIN_ELECTRIC_RPM) + MIN_ELECTRIC_RPM;

            return VehicleEvent.builder()
                    .eventType(eventType)
                    .vehicleId(vehicleId)
                    .timestamp(eventTimestamp)
                    .region(region)
                    .measurement(rpm)
                    .build();
        }

        private static VehicleEvent stoppedMotorEvent(String vehicleId, long eventTimestamp, String region) {
            EventType eventType = (rollDice() < 0.5) ? EventType.IC_RPM : EventType.ELECTRIC_RPM;
            return VehicleEvent.builder()
                    .eventType(eventType)
                    .vehicleId(vehicleId)
                    .timestamp(eventTimestamp)
                    .region(region)
                    .measurement(0)
                    .build();
        }

        private VehicleEvent currentWarningEvent(int vehicleIndex, String vehicleId, long eventTimestamp, String region) {
            return VehicleEvent.builder()
                    .eventType(WARNINGS)
                    .vehicleId(vehicleId)
                    .timestamp(eventTimestamp)
                    .region(region)
                    .measurement(getWarnings(vehicleIndex))
                    .build();
        }

        @Override
        public VehicleEvent apply(Long sourceInputValue, KeyIndexSplit keyIndexSplit) {
            // Source input value is not used

            // Generates a random vehicle index in the key split
            int vehicleIndex = RandomUtils.nextInt(keyIndexSplit.getStartKeyIndex(), keyIndexSplit.getEndKeyIndex());

            // Semi-random event for this vehicle
            VehicleEvent vehicleEvent = nextEvent(vehicleIndex);

            LOG.trace("Next event: {}", vehicleEvent);
            return vehicleEvent;
        }

        private VehicleEvent nextEvent(int vehicleIndex) {
            String vehicleId = vehicleId(vehicleIndex);
            String region = getVehicleRegion(vehicleIndex);
            long timestamp = System.currentTimeMillis();

            // The vehicle is in motion
            if (isInMotion(vehicleIndex)) {

                // Stop?
                if (rollDice() < config.probabilityMotionStateChange) {
                    LOG.debug("Stopping vehicle {}", vehicleId);

                    // Change motion state to stopped
                    vehicleInMotion.put(vehicleIndex, false);

                    // Return a stopped motor event
                    return stoppedMotorEvent(vehicleId, timestamp, region);
                }

                // Change warning state?
                if (rollDice() < config.probabilityWarningChange) {

                    // Change the number of warnings
                    int currentWarnings = getWarnings(vehicleIndex);
                    if (currentWarnings <= 0) {
                        // Set to 1
                        warnings.put(vehicleIndex, 1);
                    } else if (currentWarnings < MAX_WARNINGS) {
                        // Increase or decrease
                        int change = RandomUtils.nextInt(0, 2) - 1;
                        warnings.put(vehicleIndex, currentWarnings + change);
                    } else {
                        // Decrease
                        warnings.put(vehicleIndex, currentWarnings - 1);
                    }
                    LOG.debug("Changing warning for {} to {}", vehicleId, warnings.get(vehicleIndex));

                    // Send Warning event
                    return currentWarningEvent(vehicleIndex, vehicleId, timestamp, region);
                }

                // Return a current warning event (10% of the times) or a random motor event
                if (rollDice() < 0.1) {
                    return currentWarningEvent(vehicleIndex, vehicleId, timestamp, region);
                } else {
                    return randomMotorEvent(vehicleId, timestamp, region);
                }
            }
            // The vehicle is not in motion
            else {
                // Start
                if (rollDice() < config.probabilityMotionStateChange) {
                    LOG.debug("Starting {}", vehicleId);

                    // Change motion state to in-motion
                    vehicleInMotion.put(vehicleIndex, true);

                    // Generate a motor event
                    return randomMotorEvent(vehicleId, timestamp, region);
                }
                // Do not start
                else {
                    // Return a stopped motor event
                    return stoppedMotorEvent(vehicleId, timestamp, region);
                }
            }
        }


    }


}
