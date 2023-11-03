package com.bw.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;

import java.time.Instant;

/*
* TableDescriptor
* */
public class Job7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql("CREATE TABLE employee (\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    c_time TIMESTAMP_LTZ(3),\n" +
                "    WATERMARK FOR c_time AS c_time - INTERVAL '10' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'number-of-rows' = '10'\n" +
                ")\n");
        Table employee = tableEnvironment.from("employee");
        //employee.printSchema();
        DataStream<Object> objectDataStream = tableEnvironment.toDataStream(
                employee,
                DataTypes.STRUCTURED(
                        Employee.class,
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT()),
                        DataTypes.FIELD("c_time", DataTypes.TIMESTAMP_LTZ(3))
                )
        );
        objectDataStream.print();
        env.execute("insert into employee select * from employee");

    }

    public static class Employee {
        private String name;
        private Integer age;
        private Instant c_time;
    }
}
