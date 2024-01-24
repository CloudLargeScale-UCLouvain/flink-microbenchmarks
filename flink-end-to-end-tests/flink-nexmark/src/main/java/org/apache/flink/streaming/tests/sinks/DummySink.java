package org.apache.flink.streaming.tests.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;


/**
 * A Sink that drops all data
 */
public class DummySink<T> extends StreamSink<T> {


    public DummySink() {
        super(new SinkFunction<T>() {

            @Override
            public void invoke(T value, Context ctx) throws Exception {}
        });
    }
}
