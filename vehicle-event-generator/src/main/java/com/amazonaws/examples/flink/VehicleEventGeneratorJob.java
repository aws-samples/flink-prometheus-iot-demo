package com.amazonaws.examples.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.function.SerializableFunction;

import com.amazonaws.examples.flink.datagen.VehicleEventGeneratorFunction;
import com.amazonaws.examples.flink.domain.VehicleEvent;
import com.amazonaws.examples.flink.monitor.EventTimeExtractor;
import com.amazonaws.examples.flink.monitor.LagAndRateMonitor;
import com.amazonaws.examples.flink.sink.JsonKafkaRecordSerializationSchema;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class VehicleEventGeneratorJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(VehicleEventGeneratorJob.class);

    private static final String DEFAULT_TOPIC_NAME = "vehicle-events";

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    // Name of the local JSON resource with the application properties in the same format as they are received from the Amazon Managed Service for Apache Flink runtime
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime or from a local resource, when the environment is local
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        if (env instanceof LocalStreamEnvironment) {
            LOGGER.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            return KinesisAnalyticsRuntime.getApplicationProperties(
                    VehicleEventGeneratorJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOGGER.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }


    private static DataGeneratorSource<VehicleEvent> createDataGeneratorSource(Properties dataGenProperties) {
        double globalEventsPerSec = (double) PropertiesUtil.getInt(dataGenProperties, "events.per.sec", -1);
        Preconditions.checkArgument(globalEventsPerSec > 0, "events.per.sec must be specified and > 0");
        int numberOfVehicles = PropertiesUtil.getInt(dataGenProperties, "vehicles", 1);
        Preconditions.checkArgument(numberOfVehicles > 0, "vehicles must be > 0");

        LOGGER.info("Data Generator: {} vehicles, {} events/sec across all subtasks", numberOfVehicles, globalEventsPerSec);
        return new DataGeneratorSource<>(
                new VehicleEventGeneratorFunction(numberOfVehicles),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(globalEventsPerSec),
                TypeInformation.of(VehicleEvent.class));
    }

    private static KafkaSink<VehicleEvent> createSink(Properties kafkaSinkProperties) {
        String bootstrapServers = kafkaSinkProperties.getProperty("bootstrap.servers");
        Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers must be specified");
        String topicName = kafkaSinkProperties.getProperty("topic", DEFAULT_TOPIC_NAME);
        SerializableFunction<VehicleEvent, String> keyExtractor = VehicleEvent::getVehicleId;
        SerializableFunction<VehicleEvent, Long> eventTimeExtractor = VehicleEvent::getTimestamp;
        LOGGER.info("Kafka sink: bootstrapServers {}, topic {}", bootstrapServers, topicName);

        KafkaRecordSerializationSchema<VehicleEvent> recordSchema = new JsonKafkaRecordSerializationSchema<>(
                topicName,
                keyExtractor,
                eventTimeExtractor);

        return KafkaSink.<VehicleEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(kafkaSinkProperties) // Pass to the Kafka client any other properties
                .setRecordSerializer(recordSchema)
                .build();
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Map<String, Properties> applicationParameters = loadApplicationProperties(env);

        // DataGenerator source
        DataGeneratorSource<VehicleEvent> source = createDataGeneratorSource(applicationParameters.get("DataGen"));

        // Kafka sink
        KafkaSink<VehicleEvent> kafkaSink = createSink(applicationParameters.get("KafkaSink"));

        // Define the data flow
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "DataGen")
                // Key-by the vehicleId to retain order by vehicle
                .keyBy(VehicleEvent::getVehicleId)
                // Measure lag and event rate
                .map(new LagAndRateMonitor<>((EventTimeExtractor<VehicleEvent>) VehicleEvent::getTimestamp)).name("Monitor")
                // Sink to Kafka
                .sinkTo(kafkaSink);

        // Execute the job
        env.execute("Vehicle Event Generator");
    }

}
