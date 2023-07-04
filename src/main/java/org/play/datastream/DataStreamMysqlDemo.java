package org.play.datastream;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class DataStreamMysqlDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromCollection(Arrays.asList("hello you", "hello me"));

        source.addSink(JdbcSink.sink("insert into t_test(from_kafka) values (?)",
                (ps, t) -> {
                    ps.setString(1, t);
                }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://gz-cynosdbmysql-grp-7lsxwr5p.sql.tencentcdb.com:26456/feedback")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("colin@2022")
                        .build()
        ));
        env.execute("DataStreamMysqlDemo");
    }
}
