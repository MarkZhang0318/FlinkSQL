package com.bw.function;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkUDTFJob1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //创建数据集
        Table table = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("username", DataTypes.STRING()),
                        DataTypes.FIELD("courses", DataTypes.STRING())
                )
                , row(1, "tom", "eng:chn:math")
                , row(2, "tal", "chn:math:phy")
        ).select(
                $("id"),
                $("username"),
                $("courses")
        );
        //注册函数
        tableEnvironment.createTemporarySystemFunction("split", splitFunction.class);
        //注册临时查询
        tableEnvironment.createTemporaryView("course", table);
        //TableAPI
        tableEnvironment.from("course")
                .leftOuterJoinLateral(call("split", $("courses")).as("unit"))
                .select(
                        $("id"),
                        $("username"),
                        $("unit")
                )
               .execute()
               .print();

    }
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class splitFunction extends TableFunction<Row> {
        public void eval(String str) {
            String[] words = str.split(":");
            for (String word : words) {
                // 输出单词
                collect(Row.of(word));
            }
        }
    }
}
