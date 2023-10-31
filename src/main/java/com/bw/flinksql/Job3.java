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
        DataStreamSource<Row> rowDataStreamSource = env.fromElements(
                Row.of("zhangsan", 95),
                Row.of("lisi", 77),
                Row.of("wangwu", 82),
                Row.of("zhangsan", 60),
                Row.of("lisi", 88),
                Row.of("zhangsan", 95),
                Row.of("zhaoliu", 57)
        );

        //依据datastream生成table
        Table table = streamTableEnvironment.fromDataStream(rowDataStreamSource).as("name", "score");

        //创建临时视图
        streamTableEnvironment.createTemporaryView("table1", table);

        //利用sql聚合函数计算成绩平均值
        Table result = streamTableEnvironment.sqlQuery("select name, avg(score) from table1 group by name");

        //转换为stream
        DataStream<Row> rowDataStream = streamTableEnvironment.toChangelogStream(result);

        rowDataStream.print();

        env.execute("job3");

    }
}
