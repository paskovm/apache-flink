package com.metodipaskov.datastream.api.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class ReductionOp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> data = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\people");
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> peopleStream = data.map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> map(String value) {
                String data[] = value.split(",");
                return new Tuple4<>(data[1], data[2], data[3], 1);
            }
        });

        // Filter males from the peopleStream
        DataStream<Tuple4<String, String, String, Integer>> males = peopleStream.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Integer> value) throws Exception {
                return value.f2.equalsIgnoreCase("Male");
            }
        });

        // Filter females from the peopleStream
        DataStream<Tuple4<String, String, String, Integer>> females = peopleStream.filter(new FilterFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, Integer> value) throws Exception {
                return value.f2.equalsIgnoreCase("Female");
            }
        });

        // Count how many times every first name appear
        DataStream<Tuple2<String, Integer>> uniqueNames = peopleStream.keyBy(0).reduce(new ReduceFunction<Tuple4<String, String, String, Integer>>() {
            @Override
            public Tuple4<String, String, String, Integer> reduce(Tuple4<String, String, String, Integer> value1, Tuple4<String, String, String, Integer> value2) throws Exception {
                return new Tuple4<>(value1.f0, value1.f1, value1.f2, (int) (value1.f3 + value2.f3));
            }
        }).map(new MapFunction<Tuple4<String, String, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple4<String, String, String, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, (int) value.f3);
            }
        });

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> test = uniqueNames
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(2), Time.seconds(1)));

        uniqueNames.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\results").setParallelism(1);
        environment.execute();
    }
}
