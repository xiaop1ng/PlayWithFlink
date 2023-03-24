package org.bf.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamDemo {

    public static void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(new Tuple2<>("key" + i, i));
        }
        // 从集合中获取 stream source
        DataStream<Tuple2<String, Integer>> flintstones  = env.fromCollection(list);

        // trans

        // sink

    }

}
