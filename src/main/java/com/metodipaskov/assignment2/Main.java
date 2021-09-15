package com.metodipaskov.assignment2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class Main {

    // ## user_id,network_name,user_IP,user_country,website, Time spent before next click
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = environment.readTextFile("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test2\\numbers");
        DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> map(String s) throws Exception {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        })
                .keyBy(t -> t.f0)
                .flatMap(new StatefulMap());
//                .flatMap(new RichFlatMapFunction<Tuple2<Long, String>, Long>() {
//                    private transient ValueState<Long> sum; // 2
//                    private transient ValueState<Long> count; //  4
//
//                    public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
//                        Long currCount = 0L;
//                        Long currSum = 0L;
//
//                        if (count.value() != null) {
//                            currCount = count.value(); //   2
//                        }
//                        if (sum.value() != null) {
//                            currSum = sum.value(); //  4
//                        }
//
//                        currCount += 1;
//                        currSum = currSum + Long.parseLong(input.f1);
//
//                        count.update(currCount);
//                        sum.update(currSum);
//
//                        if (currCount >= 10) {
//                            /* emit sum of last 10 elements */
//                            out.collect(sum.value());
//                            /* clear value */
//                            count.clear();
//                            sum.clear();
//                        }
//                    }
//
//                    public void open(Configuration conf) {
//                        ValueStateDescriptor<Long> descriptor =
//                                new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {
//                                }));
//
//                        sum = getRuntimeContext().getState(descriptor);
//
//                        ValueStateDescriptor<Long> descriptor2 =
//                                new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>() {
//                                }));
//
//                        count = getRuntimeContext().getState(descriptor2);
//                    }
//                });

//        sum.writeAsText("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test2\\sum").setParallelism(1);

        sum.addSink(StreamingFileSink
                .forRowFormat(new Path("C:\\Users\\A474447\\Downloads\\apache-flink\\src\\main\\resources\\test2\\sum"), new SimpleStringEncoder<Long>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // execute program
        environment.execute("State");
    }

    static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
        private transient ValueState<Long> sum; // 2
        private transient ValueState<Long> count; //  4

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currCount = 0L;
            Long currSum = 0L;

            if (count.value() != null) {
                currCount = count.value(); //   2
            }
            if (sum.value() != null) {
                currSum = sum.value(); //  4
            }

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 10) {
                /* emit sum of last 10 elements */
                out.collect(sum.value());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }

        public void open(Configuration conf) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {
                    }));

            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 =
                    new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>() {
                    }));

            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
