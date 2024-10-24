package com.amazonaws.examples.flink.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * A {@link KafkaRecordSerializationSchema} that serializes the JSON Kafka message
 *
 * @param <T> the type of the element to be serialized
 */
public class JsonKafkaRecordSerializationSchema<T> implements KafkaRecordSerializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final SerializableSupplier<ObjectMapper> mapperFactory;
    private final SerializableFunction<T, String> keyExtractor;
    private final SerializableFunction<T, Long> eventTimeExtractor;
    private final String topicName;

    protected transient ObjectMapper mapper;

    public JsonKafkaRecordSerializationSchema(String topicName, SerializableFunction<T, String> keyExtractor, SerializableFunction<T, Long> eventTimeExtractor) {
        this.keyExtractor = keyExtractor;
        this.eventTimeExtractor = eventTimeExtractor;
        this.topicName = topicName;
        this.mapperFactory = ObjectMapper::new;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        mapper = mapperFactory.get();
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, KafkaSinkContext context, Long timestamp) {
        try {
            long eventTime = eventTimeExtractor.apply(element);
            String key = keyExtractor.apply(element);
            return new ProducerRecord<>(
                    topicName,
                    null, // choosing not to specify the partition
                    eventTime,
                    key.getBytes(StandardCharsets.UTF_8),
                    mapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
