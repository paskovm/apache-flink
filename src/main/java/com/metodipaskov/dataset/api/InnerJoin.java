package com.metodipaskov.dataset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

// input arguments:
// --input1 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\person.txt --input2 file:///C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources\location.txt --output file:\\\C:\Users\Metodi\IdeaProjects\apache-flink\src\main\resources


public class InnerJoin {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        ExecutionEnvironment environment = LocalEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        environment.getConfig().setGlobalJobParameters(parameters);

        // Read person file
        DataSet<Tuple2<Integer, String>> personSet =
                environment.readTextFile(parameters.get("input1"))
                        .map(new MapFunction<String, Tuple2<Integer, String>>()                                      //presonSet = tuple of (1  John)
                        {
                            public Tuple2<Integer, String> map(String value) {
                                String[] words = value.split(",");                                                 // words = [ {1} {John}]
                                return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                            }
                        });

        // Read location file
        DataSet<Tuple2<Integer, String>> locationSet =
                environment.readTextFile(parameters.get("input2"))
                        .map(new MapFunction<String, Tuple2<Integer, String>>() {                                                                                                 //locationSet = tuple of (1  DC)
                            public Tuple2<Integer, String> map(String value) {
                                String[] words = value.split(",");
                                return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                            }
                        });

        // Join DataSets on person id
        // The new format will be -> id, person name, state
        DataSet<Tuple3<Integer, String, String>> joinedData =
                personSet.join(locationSet)
                        .where(0)
                        .equalTo(0)
                        .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {

                            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
                                return new Tuple3<Integer, String, String>(person.f0, person.f1, location.f1);         // returns tuple of (1 John DC)
                            }
                        });

        if (parameters.get("output") != null) {
            joinedData.writeAsCsv(parameters.get("output"), "\n", " ");
            environment.execute("Join Example");
        }
    }
}
