package com.metodipaskov.datastream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationOperation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\average2");
        DataStream<Tuple5<String, String, String,Integer, Integer>> dataStream = data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
            @Override
            public Tuple5<String, String, String, Integer, Integer> map(String value) {
                String[] values = value.split(",");
                return new Tuple5<>(values[1], values[2], values[3], Integer.parseInt(values[4]), 1);
            }
        });

        dataStream.keyBy(t -> t.f0).sum(3).writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\outSum").setParallelism(1);
        dataStream.keyBy(t -> t.f0).min(3).writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\outMin").setParallelism(1);
        dataStream.keyBy(t -> t.f0).minBy(3).writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\outMinBy").setParallelism(1);
        dataStream.keyBy(t -> t.f0).max(3).writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\outMax").setParallelism(1);
        dataStream.keyBy(t -> t.f0).maxBy(3).writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\outMaxBy").setParallelism(1);

        environment.execute();
    }


}
