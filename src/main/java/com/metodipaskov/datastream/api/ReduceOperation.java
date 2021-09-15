package com.metodipaskov.datastream.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceOperation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\average");

        // month, product, category, profit, count
//        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
//            @Override
//            public Tuple5<String, String, String, Integer, Integer> map(String values){
//                String[] words = values.split(",");
//                return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
//            }
//        });
//
//        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(0)
//                .reduce(new ReduceFunction<Tuple5<String, String, String, Integer, Integer>>() {
//            @Override
//            public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, Tuple5<String, String, String, Integer, Integer> pre_result) {
//                return new Tuple5<>(current.f0, current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
//            }
//        });
//
//        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>() {
//            @Override
//            public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> values) throws Exception {
//                return new Tuple2<>(values.f0, (double) (values.f4 * 1.0 / values.f4));
//            }
//        });

        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12,1]
        //        [June,Category4,Perfume,10,1]

        // groupBy 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped.keyBy(0).reduce(new Reduce1());
        // June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce
        // reduced = { [Category4,Perfume,22,2] ..... }
        // month, avg. profit
        DataStream<Tuple2<String, Double>> profitPerMonth = reduced.map(new MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>>()
        {
            public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> input)
            {
                return new Tuple2<String, Double>(input.f0, new Double((input.f3*1.0)/input.f4));
            }
        });

//        mapped.print("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\");
//        profitPerMonth.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\");
        reduced.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\");
        environment.execute();

    }

    public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_result)
        {
            return new Tuple5<String, String, String, Integer, Integer>(current.f0,current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
        }
    }

    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>>
    {
        public Tuple5<String, String, String, Integer, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1);
        }                                                            //    June    Category5      Bat                      12
    }
}
