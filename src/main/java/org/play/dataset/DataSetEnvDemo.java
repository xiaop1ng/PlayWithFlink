package org.play.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

public class DataSetEnvDemo {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Integer> dataSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        dataSource.print();
    }

}
