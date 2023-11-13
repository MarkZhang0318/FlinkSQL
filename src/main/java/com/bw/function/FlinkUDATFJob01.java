package com.bw.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.FlatAggregateTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkUDATFJob01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //创建数据集
        Table table = tableEnvironment.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("username", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.DOUBLE())
                )
                , row(1, "tom", 85.0)
                , row(2, "tal", 97.0)
                , row(3, "sam", 89.0)
                , row(4, "alice", 99.0)
        ).select(
                $("id"),
                $("username"),
                $("score")
        );

        //注册聚合函数
        tableEnvironment.createTemporarySystemFunction("Top2Aggregation", new Top2AggregationFunction());

        //注册临时视图
        tableEnvironment.createTemporaryView("tab1", table);
        //TableAPI调用
        table.flatAggregate(call("Top2Aggregation", $("score")).as("top_score", "rank"))
                .select(
                        $("top_score"),
                        $("rank")
                ).leftOuterJoin(table, $("top_score").isEqual($("score")))
                .select(
                        $("id"),
                        $("username"),
                        $("score"),
                        $("rank")

                )
                .execute()
                .print();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Top2Accumulator {
        private Double first = Double.MIN_VALUE;
        private Double second = Double.MIN_VALUE;
    }

    public static class Top2AggregationFunction extends TableAggregateFunction<Tuple2<Double, Integer>, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            return new Top2Accumulator();
        }

        public void accumulate(Top2Accumulator accumulator, Double value) {
            if (value > accumulator.getFirst()) {
                accumulator.setSecond(accumulator.getFirst());
                accumulator.setFirst(value);
            }
            if (value > accumulator.getSecond() && value < accumulator.getFirst()) {
                accumulator.setSecond(value);
            }
        }

        public void emitValue(Top2Accumulator accumulator, Collector<Tuple2<Double, Integer>> out) {
            if (accumulator.getFirst() != Double.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.getFirst(), 1));

            }
            if (accumulator.getSecond() != Double.MIN_VALUE) {
                out.collect(Tuple2.of(accumulator.getSecond(), 2));
            }
        }
    }
}
