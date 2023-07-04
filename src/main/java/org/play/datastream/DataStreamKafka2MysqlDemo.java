package org.play.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;


public class DataStreamKafka2MysqlDemo {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                        .setBootstrapServers("localhost:9092")
                        .setTopics("topic-test")
                        .setGroupId("my-group")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
        DataStream<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        source.print();

        source.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                super.invoke(value, context);
                System.out.println("invoke: " + value);
                // 数据库连接信息
                String url = "jdbc:mysql://gz-cynosdbmysql-grp-7lsxwr5p.sql.tencentcdb.com:26456/feedback";
                String username = "flink";
                String password = "flink@2023";
                String sql = " insert into t_test(from_kafka) values ( '" + value + "' ) ";
                try {
                    // 加载 MySQL 驱动程序
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    // 建立数据库连接
                    Connection connection = DriverManager.getConnection(url, username, password);
                    // 创建 statement 对象
                    Statement statement = connection.createStatement();
                    int i = statement.executeUpdate(sql);
                    System.out.println("update: " +  i);
                    // 关闭资源
                    statement.close();
                    connection.close();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        });

        env.execute("DataStreamKafkaDemo");
    }

}
