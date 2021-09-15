package com.metodipaskov.dataset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

// --input1 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\person --input2 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\location --output file:\\\C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources

public class LeftJoin {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(parameters);

        DataSet<Tuple2<Integer, String>> personSet = environment.readTextFile(parameters.get("input1"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] values = value.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(values[0]), values[1]);
                    }
                });

        DataSet<Tuple2<Integer, String>> locationSet = environment.readTextFile(parameters.get("input2"))
                .map(new MapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        String[] values = value.split(",");
                        return new Tuple2<Integer, String>(Integer.parseInt(values[0]), values[1]);
                    }
                });

        DataSet<Tuple3<Integer, String, String>> join = personSet.leftOuterJoin(locationSet)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                        if (location == null)
                            return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");

                        return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);
                    }
                });

        join.writeAsCsv(parameters.get("output"), "\n", " ");
        environment.execute();
    }
}
