package com.bw.function;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkUDFJob02 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        Table table = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("username", DataTypes.STRING()),
                        DataTypes.FIELD("hobbies", DataTypes.STRING())
                )
                , row(1, "tom", "basketball,swimming")
                , row(2, "sam", "swimming,reading")
                , row(3, "tal", "running,basketball")
        ).select(
                $("id"),
                $("username"),
                $("hobbies")
        );
        tableEnvironment.createTemporaryView("user_table", table);
        //使用方式1, 直接用select 和call 调用
        table.select($("id"), $("username"), call(new substrFunction(false), $("hobbies"), 0, 5))
                .execute().print();

        //使用方式2, 先注册函数，然后用sql调用
        tableEnvironment.createTemporarySystemFunction("substr", new substrFunction(true));
        tableEnvironment.sqlQuery("select id,username,substr(hobbies,0,5) as hob_substr from user_table").execute().print();
    }

    public static class substrFunction extends ScalarFunction {
        private boolean include;

        public substrFunction(boolean include) {
            this.include = include;
        }

        public String eval(String str, int start, int end) {
            return include ? str.substring(start, end + 1) : str.substring(start, end);
        }
    }



}
