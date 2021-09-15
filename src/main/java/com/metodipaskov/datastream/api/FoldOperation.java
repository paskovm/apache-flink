package com.metodipaskov.datastream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FoldOperation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\average");
        DataStream<Tuple5<String, String, String,Integer, Integer>> dataStream = data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> map(String value) {
                String[] values = value.split(",");
                return new Tuple5<>(values[1], values[2], values[3], Integer.parseInt(values[4]), 1);
            }
        });

        // Fold option not present enymore
//        DataStream<Tuple4<String, String, Integer, Integer>> foldedStream = dataStream.keyBy(0).



        dataStream.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\fold").setParallelism(1);
        environment.execute("Fold option");
    }
}
