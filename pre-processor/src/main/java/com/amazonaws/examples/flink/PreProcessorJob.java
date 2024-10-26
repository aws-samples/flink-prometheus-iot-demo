package com.amazonaws.examples.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.RetryConfiguration;
import org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.SinkWriterErrorHandlingBehaviorConfiguration;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeriesLabelsAndMetricNameKeySelector;
import org.apache.flink.connector.prometheus.sink.aws.AmazonManagedPrometheusWriteRequestSigner;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import com.amazonaws.examples.flink.aggregate.VehiclesInMotionProcessWindowFunction;
import com.amazonaws.examples.flink.aggregate.WarningsProcessWindowFunction;
import com.amazonaws.examples.flink.domain.AggregateVehicleEvent;
import com.amazonaws.examples.flink.domain.EnrichedVehicleEvent;
import com.amazonaws.examples.flink.domain.EventType;
import com.amazonaws.examples.flink.domain.VehicleEvent;
import com.amazonaws.examples.flink.enrich.VehicleModelEnrichmentFunction;
import com.amazonaws.examples.flink.filter.IncludeEventTypes;
import com.amazonaws.examples.flink.map.AggregateEventsToPrometheusTimeSeriesMapper;
import com.amazonaws.examples.flink.monitor.EventTimeExtractor;
import com.amazonaws.examples.flink.monitor.LagAndRateMonitor;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.prometheus.sink.PrometheusSinkConfiguration.OnErrorBehavior.DISCARD_AND_CONTINUE;

public class PreProcessorJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(PreProcessorJob.class);

    private static final String DEFAULT_TOPIC_NAME = "vehicle-events";
    private static final String DEFAULT_CONSUMER_GROUP_ID = "pre-processor";
    private static final int DEFAULT_MAX_REQUEST_RETRY = 100;

    private static final int DEFAULT_AGGREGATION_WINDOW_SEC = 5;

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

    private static <T> KafkaSource<T> createKafkaSource(Properties kafkaSourceProperties, Class<T> recordClass) {
        String bootstrapServers = kafkaSourceProperties.getProperty("bootstrap.servers");
        Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers not defined");
        String topicName = kafkaSourceProperties.getProperty("topic", DEFAULT_TOPIC_NAME);
        String consumerGroupId = kafkaSourceProperties.getProperty("group.id", DEFAULT_CONSUMER_GROUP_ID);
        LOGGER.info("Kafka source: bootstrapServers {}, topic {}, consumerGroup {}", bootstrapServers, topicName, consumerGroupId);

        JsonDeserializationSchema<T> jsonFormat = new JsonDeserializationSchema<>(recordClass);
        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topicName)
                .setGroupId(consumerGroupId)
                .setProperties(kafkaSourceProperties)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(jsonFormat)
                .build();
    }

    private static Sink<PrometheusTimeSeries> createPrometheusSink(Properties prometheusSinkProperties) {
        String endpointUrl = prometheusSinkProperties.getProperty("endpoint.url");
        Preconditions.checkNotNull(endpointUrl, "endpoint.url not defined");
        String awsRegion = prometheusSinkProperties.getProperty("aws.region", new DefaultAwsRegionProviderChain().getRegion().toString());
        int maxRequestRetryCount = PropertiesUtil.getInt(prometheusSinkProperties, "max.request.retry", DEFAULT_MAX_REQUEST_RETRY);
        Preconditions.checkArgument(maxRequestRetryCount > 0, "max.request.retry must be > 0");
        LOGGER.info("Prometheus sink: endpoint {}, region {}", endpointUrl, awsRegion);

        return PrometheusSink.builder()
                .setPrometheusRemoteWriteUrl(endpointUrl)
                .setRequestSigner(new AmazonManagedPrometheusWriteRequestSigner(endpointUrl, awsRegion))
                .setRetryConfiguration(RetryConfiguration.builder()
                        .setMaxRetryCount(maxRequestRetryCount).build())
                .setErrorHandlingBehaviorConfiguration(SinkWriterErrorHandlingBehaviorConfiguration.builder()
                        .onMaxRetryExceeded(DISCARD_AND_CONTINUE)
                        .build())
                .setMetricGroupName("kinesisAnalytics") // Forward connector metrics to CloudWatch
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> applicationParameters = loadApplicationProperties(env);
        int applicationParallelism = env.getParallelism();

        if (isLocal(env)) {
            env.setParallelism(4);
        }


        /// Define the data flow

        int kafkaSourceMaxParallelism = PropertiesUtil.getInt(applicationParameters.get("KafkaSource"), "max.parallelism", Integer.MAX_VALUE);
        DataStream<EnrichedVehicleEvent> enrichedVehicleEvents = env
                // Read raw events from Kafka
                .fromSource(
                        createKafkaSource(applicationParameters.get("KafkaSource"), VehicleEvent.class),
                        // No watermark needed for processing-time semantics
                        WatermarkStrategy.noWatermarks(),
                        "KafkaSource"
                ).uid("kafka-source")
                // Optionally limit the parallelism of the source, to be <= number of partitions in the Kafka topic
                .setParallelism(Math.min(applicationParallelism, kafkaSourceMaxParallelism))
                // Enrich adding vehicle model
                .map(new VehicleModelEnrichmentFunction()).name("EnrichWithModel").uid("enrich-model");

        int aggregationWindowSec = PropertiesUtil.getInt(
                applicationParameters.get("Aggregation"), "window.size.sec", DEFAULT_AGGREGATION_WINDOW_SEC);

        DataStream<AggregateVehicleEvent> aggregateVehicleInMotion = enrichedVehicleEvents
                // Only include motor events
                .filter(new IncludeEventTypes(EventType.IC_RPM, EventType.ELECTRIC_RPM))
                // Aggregate counting vehicles in motion, per model and per region, every `aggregationWindowSec` seconds
                .keyBy(evt -> evt.getVehicleModel() + evt.getRegion()) // Partition by vehicle model and region
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(aggregationWindowSec)))
                //.aggregate(new VehiclesInMotionAggregateFunction()) // alternative implementation
                .process(new VehiclesInMotionProcessWindowFunction())
                .name("AggregateVehiclesInMotion").uid("aggregate-in-motion");

        DataStream<AggregateVehicleEvent> aggregateWarnings = enrichedVehicleEvents
                // Only include warning events
                .filter(new IncludeEventTypes(EventType.WARNINGS))
                // Aggregate counting warnings, per model and per region, every `aggregationWindowSec` seconds
                .keyBy(evt -> evt.getVehicleModel() + evt.getRegion()) // Partition by vehicle model and region
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(aggregationWindowSec)))
                //.aggregate(new WarningsAggregateFunction()) // alternative implementation
                .process(new WarningsProcessWindowFunction())
                .name("AggregateWarnings").uid("aggregate-warnings");

        aggregateVehicleInMotion
                // Merge the two streams
                .union(aggregateWarnings)
                // Measure lag and event rate
                .map(new LagAndRateMonitor<>((EventTimeExtractor<AggregateVehicleEvent>) AggregateVehicleEvent::getTimestamp)).name("Monitor")
                // Map records to Prometheus sink input records
                .flatMap(new AggregateEventsToPrometheusTimeSeriesMapper()).name("MapWarningsToPromTS")
                // Key-by time series to ensure order is retained
                .keyBy(new PrometheusTimeSeriesLabelsAndMetricNameKeySelector()) // Key-by time series to ensure order is retained
                // Attach
                .sinkTo(createPrometheusSink(applicationParameters.get("PrometheusSink"))).name("PrometheusSink").uid("prometheus-sink");


        // Execute the job
        env.execute("Vehicle Event Pre-processor");
    }
}