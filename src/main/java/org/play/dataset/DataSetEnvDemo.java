package org.play.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

public class DataSetEnvDemo {

    public static void main(String[] args) throws Exception{
        // 1. get env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. source
        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        // 3. operation
        source.print();
    }

}
