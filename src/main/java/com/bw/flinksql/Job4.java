package com.bw.flinksql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
* Append-only流
* streaming模式下过来一条数据处理一条数据，需要更新就删除旧数据更新新数据
* batch模式下将过来的一批数据一次性处理得到最终结果再输出
* */
public class Job4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

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

        //依据DataStream生成table
        Table table = streamTableEnvironment.fromDataStream(rowDataStreamSource).as("name", "score");

        //依据Table生成临时视图
        streamTableEnvironment.createTemporaryView("table1", table);

        //执行SQL，生成结果表
        Table result = streamTableEnvironment.sqlQuery("select name, avg(score) from table1 group by name");

        //再将生成的结果变回stream
        DataStream<Row> rowDataStream = streamTableEnvironment.toChangelogStream(result);

        //输出结果
        rowDataStream.print();

        env.execute("job4");
        /*
        * STREAMING模式下：
        * +I[zhangsan, 95]
          +I[lisi, 77]
          +I[wangwu, 82]
          -U[zhangsan, 95]
          +U[zhangsan, 77]
          -U[lisi, 77]
          +U[lisi, 82]
          -U[zhangsan, 77]
          +U[zhangsan, 83]
          +I[zhaoliu, 57]
        * */

        /*
        * BATCH模式下：
        *+I[zhangsan, 83]
         +I[lisi, 82]
         +I[wangwu, 82]
         +I[zhaoliu, 57]
        * */

    }
}
