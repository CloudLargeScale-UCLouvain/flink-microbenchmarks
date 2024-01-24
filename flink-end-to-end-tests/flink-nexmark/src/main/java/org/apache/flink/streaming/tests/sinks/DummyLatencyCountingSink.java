package org.apache.flink.streaming.tests.sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;

import org.slf4j.Logger;


/**
 * A Sink that drops all data and periodically emits latency measurements
 */
public class DummyLatencyCountingSink<T> extends StreamSink<T> {

    private final Logger logger;

    public DummyLatencyCountingSink(Logger log) {
        super(new SinkFunction<T>() {

            @Override
            public void invoke(T value, Context ctx) throws Exception {}
        });
        logger = log;
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        logger.warn("%{}%{}%{}%{}%{}%{}", "latency",
                System.currentTimeMillis() - latencyMarker.getMarkedTime(), System.currentTimeMillis(), latencyMarker.getMarkedTime(),
                latencyMarker.getSubtaskIndex(), getRuntimeContext().getIndexOfThisSubtask());
    }
}