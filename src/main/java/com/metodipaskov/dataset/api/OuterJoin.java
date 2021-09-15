package com.metodipaskov.dataset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class OuterJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> personSet = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\person")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String values) throws Exception {
                        String[] people = values.split(",");
                        return new Tuple2<>(Integer.parseInt(people[0]), people[1]);
                    }
                });

        DataSet<Tuple2<Integer, String>> locationSet = environment.readTextFile("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources\\location")
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String values) throws Exception {
                        String[] locations = values.split(",");
                        return new Tuple2<>(Integer.parseInt(locations[0]), locations[1]);
                    }
                });

        DataSet<Tuple3<Integer, String, String>> outerJoin = personSet.fullOuterJoin(locationSet, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> people, Tuple2<Integer, String> locations) throws Exception {
                        if (locations == null) {
                            return new Tuple3<>(people.f0, people.f1, "NULL");
                        } else if (people == null){
                            return new Tuple3<>(locations.f0, "NULL", locations.f1);
                        }

                        return new Tuple3<>(people.f0, people.f1, locations.f1);
                    }
                });

        if (outerJoin != null) {
            outerJoin.writeAsCsv("C:\\Users\\Metodi\\IdeaProjects\\apache-flink\\src\\main\\resources", "\n", " ");
            environment.execute("Outer Join is executed");
        }

    }
}
