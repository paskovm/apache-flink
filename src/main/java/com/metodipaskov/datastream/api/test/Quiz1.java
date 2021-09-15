package com.metodipaskov.datastream.api.test;

//Data is of the following schema
//
//        # cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
//        Using Datastream/Dataset transformations find the following for each ongoing trip.
//
//        1.) Popular destination.  | Where more number of people reach.
//        2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
//        3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Set;

public class Quiz1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = environment.readTextFile("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test\\cab+info");

        // 1. cab id, 2. cab number plate, 3. cab type, 4. cab driver name, 5. ongoing trip/not, 6. pickup location, 7. destination, 8. passenger count
        DataStream<Tuple8<String, String, String, String, String, String, String, Integer>> dataStream =
                data.map(new MapFunction<String, Tuple8<String, String, String, String, String, String, String, Integer>>() {
                    @Override
                    public Tuple8<String, String, String, String, String, String, String, Integer> map(String value) {
                        String[] values = value.split(",");

                        int lastValue;
                        if (values[7].equalsIgnoreCase("'null'")) {
                            lastValue = 0;
                        } else {
                            lastValue = Integer.parseInt(values[7]);
                        }
                        return new Tuple8<>(values[0], values[1], values[2], values[3], values[4], values[5], values[6], lastValue);
                    }
                });

        // Filter destinations only
        DataStream<Tuple2<String, Integer>> dataStreamOfDestinations = dataStream.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>,
                Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> inputStream) {

                return new Tuple2<>(inputStream.f5, inputStream.f7);
            }
        }).filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> inputStream) throws Exception {
                return !inputStream.f0.contains("null");
            }
        });

        DataStream<Tuple2<String, Integer>> mapDestinations = dataStreamOfDestinations.keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> current, Tuple2<String, Integer> pre_result) {
                        return new Tuple2<>(current.f0, current.f1 + pre_result.f1);
                    }
                }).keyBy(0).maxBy(1);

        // ============================================= 2 =============================================

        DataStream<Tuple3<String, Integer, Integer>> pickUpDestinations = dataStream.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> values) throws Exception {
                return new Tuple3<>(values.f5, values.f7, 1);
            }
        }).filter(new FilterFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<String, Integer, Integer> values) throws Exception {
                return !values.f0.contains("null");
            }
        }).keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current, Tuple3<String, Integer, Integer> previous) throws Exception {
                        return new Tuple3<>(current.f0, current.f1 + previous.f1, current.f2 + previous.f2);
                    }
                });

        DataStream<Tuple2<String, Double>> averageNumberOfPassengers = pickUpDestinations.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, Double>>() {
            @Override
            public Tuple4<String, Integer, Integer, Double> map(Tuple3<String, Integer, Integer> values) {
                return new Tuple4<>(values.f0, values.f1, values.f2, Double.valueOf(values.f1) / Double.valueOf(values.f2));
            }
        }).map(new MapFunction<Tuple4<String, Integer, Integer, Double>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple4<String, Integer, Integer, Double> values) throws Exception {
                return new Tuple2<>(values.f0, values.f3);
            }
        });


        // ============================================= 3 =============================================

        // take tuple8 -> 1. Tuple3 map new field to add 1 -> 2. filter all nulls -> 3. Reduce to add/delete numbers -> 4. Map to Tuple2 String, Double

        DataStream<Tuple3<String, Integer, Integer>> collectTripsPerDriver = dataStream.map(new MapFunction<Tuple8<String, String, String, String, String, String, String, Integer>, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Tuple8<String, String, String, String, String, String, String, Integer> values) {
                return new Tuple3<>(values.f3, values.f7, 1);
            }
        }).filter(new FilterFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<String, Integer, Integer> values) {
                return values.f1 > 0;
            }
        }).keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current, Tuple3<String, Integer, Integer> previous) throws Exception {
                        return new Tuple3<>(current.f0, current.f1 + previous.f1, current.f2 + previous.f2);
                    }
                });

        DataStream<Tuple2<String, String>> averageTripsPerDriver = collectTripsPerDriver.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple3<String, Integer, Integer> values) throws Exception {
                return new Tuple2<>(values.f0, String.format("%.02f",  Double.valueOf(values.f1) / Double.valueOf(values.f2)));
            }
        });

        mapDestinations.writeAsCsv("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test\\destinations").setParallelism(1);
        averageNumberOfPassengers.writeAsCsv("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test\\average").setParallelism(1);
        averageTripsPerDriver.writeAsCsv("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test\\averageTripPerDriver").setParallelism(1);
        environment.execute();


    }
}
