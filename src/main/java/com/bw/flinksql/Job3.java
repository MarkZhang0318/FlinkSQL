package com.bw.flinksql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
* Append-only流
* */
public class Job3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);
        //创建一条datastream
        DataStreamSource<Tuple2<String, String>> tuple2DataStreamSource = env.fromElements(
                Tuple2.of("zhangsan", "84L"),
                Tuple2.of("lisi", "95L"),
                Tuple2.of("zhangsan", "99L"),
                Tuple2.of("lisi", "73L"),
                Tuple2.of("zhangsan", "66L")
                );
        //根据这个DataStream创建一张表
        Table table = streamTableEnvironment.fromDataStream(tuple2DataStreamSource);
        streamTableEnvironment.createTemporaryView("table1", table);

        //执行SQL
        Table table1 = streamTableEnvironment.sqlQuery("select * from table1");

        //再将其转换为一个datastream
        DataStream<Row> rowDataStream = streamTableEnvironment.toDataStream(table1);

        rowDataStream.print();

        env.execute("job2");

    }
}
