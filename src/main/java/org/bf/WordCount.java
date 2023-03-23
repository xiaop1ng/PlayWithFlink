package org.bf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WordCount {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WordCount.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/root/app/flink/hello.txt";

        DataSet<String> ds = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> resultSet = ds.flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);
        resultSet.print();
    }


    private static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            // 遍历所有word，包成二元组输出
            for (String str : words) {
                collector.collect(new Tuple2<>(str, 1));
            }
        }
    }
}
