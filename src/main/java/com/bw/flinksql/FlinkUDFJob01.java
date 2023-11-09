package com.bw.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class FlinkUDFJob01 {

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
        //注册函数
        tableEnvironment.createTemporarySystemFunction("substr", SubstrFunction.class);
        tableEnvironment.createTemporaryView("myView", table);

        tableEnvironment.sqlQuery("select substr(payload, 0, 3) as substr from myView limit 10")
               .execute()
               .print();
    }
    public static class SubstrFunction extends ScalarFunction {
        public String eval(String str, Integer start, Integer end) {
            return str.substring(start, end);
        }
    }
}

