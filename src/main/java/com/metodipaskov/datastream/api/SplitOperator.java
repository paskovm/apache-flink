package com.metodipaskov.datastream.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class SplitOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> numbers = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\numbers");

        SingleOutputStreamOperator<Tuple2<String, Integer>> nums = numbers.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {

                return new Tuple2<>(null, Integer.parseInt(value));
            }
        }).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String str;
                if (value.f1 % 2 == 0) {
                    str = "even";
                } else {
                    str = "odd";
                }

                out.collect(new Tuple2<>(str, value.f1));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> evenNum = nums.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) {
                return value.f0.equalsIgnoreCase("even");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> oddNum = nums.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) {
                return value.f0.equalsIgnoreCase("odd");
            }
        });

        evenNum.writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\even").setParallelism(1);
        oddNum.writeAsText("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\odd").setParallelism(1);
        environment.execute();

    }
}
