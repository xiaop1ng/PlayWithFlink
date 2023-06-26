package org.play.datastream;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class DataStreamEnvDemo {

    public static void main(String[] args) throws Exception{

        // 1. get env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. source
        DataStreamSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        // 3. define task
        source.print();

        // 4. execute task
        env.execute("DataStreamEnvDemo");
    }

}
