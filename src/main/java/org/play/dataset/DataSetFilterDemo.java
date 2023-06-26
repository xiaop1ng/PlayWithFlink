package org.play.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

public class DataSetFilterDemo {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        // 输出偶数
        source.filter(i -> i%2==0).setParallelism(1).print();

    }

}
