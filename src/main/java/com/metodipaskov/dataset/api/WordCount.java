package com.metodipaskov.dataset.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;


// input arguments:
// --input file:///home/devuser/IdeaProjects/apache-flink-wordcout/src/main/resources/wordcount.txt --output file:///home/devuser/IdeaProjects/apache-flink-wordcout/src/main/resources


public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = environment.readTextFile(params.get("input"));
        DataSet<String> filtered = text.filter((FilterFunction<String>) value -> value.startsWith("N"));

        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
        DataSet<Tuple2<String, Integer>> wordCounts = tokenized.groupBy(new int[]{0}).sum(1);

        if (params.has("output")) {
            wordCounts.writeAsCsv(params.get("output"), "\n", " ");
            environment.execute();
        }

    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            return new Tuple2<>(value, Integer.valueOf(1));
        }
    }
}
