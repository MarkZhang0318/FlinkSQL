package com.bw.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkUDAFJob01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //创建数据集
        Table table = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("username", DataTypes.STRING()),
                        DataTypes.FIELD("course", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT())
                )
                , row(1, "tom", "eng", 90)
                , row(1, "tom", "chn", 80)
                , row(2, "tal", "eng", 85)
                , row(2, "tal", "math", 88)
        ).select(
                $("id"),
                $("username"),
                $("course"),
                $("score")
        );
        //注册函数
        tableEnvironment.createTemporarySystemFunction("avg", avgFunction.class);
        //调用函数得出结果
        table.groupBy($("id"), $("username"))
               .select($("id"), $("username"), call("avg", $("score")).as("avgScore"))
               .execute()
               .print();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Aggregator {
        private Double sum;
        private Integer count;
    }

    public static class avgFunction extends AggregateFunction<Integer, Aggregator> {
        @Override
        public Aggregator createAccumulator() {
            return new Aggregator(0.0, 0);
        }

        public void accumulate(Aggregator aggregator, Integer value) {
            aggregator.setSum(aggregator.getSum() + value);
            aggregator.setCount(aggregator.getCount() + 1);
        }

        @Override
        public Integer getValue(Aggregator aggregator) {
            return aggregator.getCount() == 0? 0 : (int) (aggregator.getSum() / aggregator.getCount());
        }


    }

}

