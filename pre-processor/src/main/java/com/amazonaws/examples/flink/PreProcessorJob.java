package com.amazonaws.examples.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.amazonaws.examples.flink.domain.VehicleEvent;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class PreProcessorJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreProcessorJob.class);

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
                    PreProcessorJob.class.getClassLoader()
                            .getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath());
        } else {
            LOGGER.info("Loading application properties from Amazon Managed Service for Apache Flink");
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }

    private static <T> KafkaSource<T> createKafkaSource(String bootstrapServers, String topicName, String consumerGroupName, Class<T> recordClass, Properties consumerProperties) {
        JsonDeserializationSchema<T> jsonFormat = new JsonDeserializationSchema<>(recordClass);
        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topicName)
                .setGroupId(consumerGroupName)
                .setProperties(consumerProperties)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(jsonFormat)
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Map<String, Properties> applicationParameters = loadApplicationProperties(env);

        if (isLocal(env)) {
            env.setParallelism(2);
        }

        // Kafka source
        KafkaSource<VehicleEvent> kafkaSource = createKafkaSource(
                applicationParameters.get("KafkaSource").getProperty("bootstrap.servers"),
                applicationParameters.get("KafkaSource").getProperty("topic", DEFAULT_TOPIC_NAME),
                applicationParameters.get("KafkaSource").getProperty("group.id"),
                VehicleEvent.class,
                applicationParameters.get("KafkaSource"));


        // Define the data flow
        DataStream<VehicleEvent> rawEvents = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .uid("kafka-source");


        rawEvents.print();

        // Execute the job
        env.execute("Vehicle Event Pre-processor");
    }
}
