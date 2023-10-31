package com.bw.flinksql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job1 {
    public static void main(String[] args) {
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

        //根据上面创建的table创建一个临时视图，可以查看表里面的内容
        //前面是临时视图的名字，后面则是用于创建视图的表
        streamTableEnvironment.createTemporaryView("myView", table);
        //打印表的schema
        streamTableEnvironment.from("myView").printSchema();


        /*
        * 此处创建另外一个临时视图，利用schema类指定列的名称和类型
        * 此处需要使用datastream，不能直接使用table
        * */
        streamTableEnvironment.createTemporaryView(
                "myView2",
                tuple2DataStreamSource,
                Schema.newBuilder().column("f0", "STRING")
                        .column("f1", "BIGINT")
                        .build()
        );

        streamTableEnvironment.from("myView2").printSchema();

        streamTableEnvironment.createTemporaryView("myView3", table.as("name", "score"));
        streamTableEnvironment.from("myView3").printSchema();
    }
}
