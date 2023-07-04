package org.play.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DataStreamKafkaDemo {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("topic-test")
                        .setGroupId("my-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        source.print();
        env.execute("DataStreamKafkaDemo");
    }

}
