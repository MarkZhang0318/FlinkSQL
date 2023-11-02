package com.bw.flinksql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
*
* */
public class Job5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //使用API中的方法直接插入另一张表，executeInsert参数为另一张表的path
        tableEnvironment.from("datasource1").executeInsert("result1");

        //使用SQL的方法
        Table datasource2 = tableEnvironment.from("datasource2");
        tableEnvironment.executeSql("INSERT INTO result2 SELECT * FROM datasource2");

        //StatementSet，可以一张表向多个结果集插入

        StreamStatementSet statementSet = tableEnvironment.createStatementSet();
        statementSet.addInsert("result3", datasource2)
                .addInsert("result4", datasource2)
                .execute();


        env.execute("job5");


    }
}
