package com.bw.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
* TableDescriptor
* */
public class Job6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Table table = tableEnvironment.from(TableDescriptor.forConnector("datagen")
                .option("number-of-rows", "50")
                .schema(Schema.newBuilder()
                        .column("uid", "TINYINT")
                        .column("payload", "STRING")
                        .build())
                .build()
        );

        tableEnvironment.toDataStream(table)
                .keyBy(t -> t.getFieldAs("uid"))
                .map(v -> v.getField("payload"))
                .executeAndCollect()
                .forEachRemaining(System.out::println);


        //env.execute("job5");


    }
}
