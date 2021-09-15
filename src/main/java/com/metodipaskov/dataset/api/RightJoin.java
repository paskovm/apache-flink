package com.metodipaskov.dataset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

// --input1 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\person --input2 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\location --output file:\\\C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources

public class RightJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> personSet = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\person")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String people) {
                        String[] persons = people.split(",");
                        return new Tuple2<>(Integer.parseInt(persons[0]), persons[1]);
                    }
                });

        DataSet<Tuple2<Integer, String>> locationSet = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\location")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String location) {
                        String[] locations = location.split(",");
                        return new Tuple2<>(Integer.parseInt(locations[0]), locations[1]);
                    }
                });

        DataSet<Tuple3<Integer, String, String>> rightJoin = personSet.rightOuterJoin(locationSet)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> people, Tuple2<Integer, String> locations) throws Exception {
                        if (people == null)
                            return new Tuple3<>(locations.f0, "NULL", locations.f1);

                        return new Tuple3<>(locations.f0, people.f1, locations.f1);
                    }
                });

        rightJoin.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources");
        environment.execute();


    }
}
