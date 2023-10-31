package com.bw.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
* Append-only流
* */
public class Job4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //创建一条datastream
        DataStreamSource<Row> rowDataStreamSource = env.fromElements(
                Row.of("zhangsan", 95),
                Row.of("lisi", 77),
                Row.of("wangwu", 82),
                Row.of("zhangsan", 60),
                Row.of("lisi", 88),
                Row.of("zhangsan", 95),
                Row.of("zhaoliu", 57)
        );



        env.execute("job4");

    }
}
