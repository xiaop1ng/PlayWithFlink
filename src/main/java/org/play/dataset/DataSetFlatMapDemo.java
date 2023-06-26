package org.play.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DataSetFlatMapDemo {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        source.flatMap(new FlatMapFunction<Integer, Object>() {
            @Override
            public void flatMap(Integer integer, Collector<Object> out) throws Exception {
                if (integer%2 == 0) {
                    out.collect(integer + " 是偶数");
                } else {
                    out.collect(integer + " 是奇数");
                }
            }
        }).setParallelism(4).print();

    }
}
