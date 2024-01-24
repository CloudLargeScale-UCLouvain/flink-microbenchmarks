package be.uclouvain.gepiciad.micro.window;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory;
import org.apache.flink.streaming.tests.Event;
import org.apache.flink.util.Collector;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.applyTumblingWindows;
import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;
import static org.apache.flink.streaming.tests.TestOperatorEnum.TIME_WINDOW_OPER;

public class ReadOnly {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid())
                        .keyBy(Event::getKey);

        WindowedStream<Event, Integer, TimeWindow> windowedStream = applyTumblingWindows(keyedStream, pt);
        windowedStream.apply(
                new WindowFunction<Event, Event, Integer, TimeWindow>() {
                    @Override
                    public void apply(
                            Integer integer,
                            TimeWindow window,
                            Iterable<Event> input,
                            Collector<Event> out) {
                        for (Event e : input) {
                            out.collect(e);
                        }
                    }
                })
                .name(TIME_WINDOW_OPER.getName())
                .uid(TIME_WINDOW_OPER.getUid());

        env.execute("RocksDB test job");
    }
}
