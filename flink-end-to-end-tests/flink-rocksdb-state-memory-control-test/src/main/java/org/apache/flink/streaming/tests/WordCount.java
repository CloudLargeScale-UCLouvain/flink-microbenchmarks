package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WordCount {



    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.addSource(new GutenbergSource());
        DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .name("tokenizer")
                .disableChaining()
                .keyBy(value -> value.f0)
                .sum(1)
                .disableChaining()
                .name("counter");
        //counts.addSink(new DiscardingSink<Tuple2<String, Integer>>());
        counts.addSink(new DiscardingSink<>());
        env.execute("WordCount");

    }

    public static class GutenbergSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

        private static final int START_INDEX = 71735;
        private static final int STOP_INDEX = 9153;
        private volatile boolean isRunning = true;

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = START_INDEX; i > STOP_INDEX & isRunning; i--) {
                URL url = new URL(String.format("https://www.gutenberg.org/cache/epub/%d/pg%d.txt", i, i));
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("GET");
                try {
                    int status = con.getResponseCode();
                    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        ctx.collect(inputLine);
                    }
                    in.close();
                } catch (FileNotFoundException ignored) {
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
