package com.bw.flinksql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class Job8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<Row> rowDataStreamSource = env.fromElements(
                Row.ofKind(RowKind.INSERT, 1, "a"),
                Row.ofKind(RowKind.INSERT, 2, "b"),
                Row.ofKind(RowKind.UPDATE_BEFORE, 1, "a"),
                Row.ofKind(RowKind.UPDATE_AFTER, 2, "a")
        );
        rowDataStreamSource.print();

        //tableEnv.createTemporaryView("myView", rowDataStreamSource);
        //tableEnv.sqlQuery("select * from myView").execute().print();
        env.execute("Job8");
    }
}
