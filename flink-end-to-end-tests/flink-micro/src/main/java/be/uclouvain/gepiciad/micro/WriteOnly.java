package be.uclouvain.gepiciad.micro;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory;
import org.apache.flink.streaming.tests.Event;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;

public class WriteOnly {

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

        keyedStream
                .map(new ValueStateMapper())
                .name("ValueStateMapper")
                .uid("ValueStateMapper")
                .addSink(new DiscardingSink<>())
                .name("DiscardingSink")
                .uid("DiscardingSink");


        env.execute("RocksDB test job");
    }


    private static class ValueStateMapper extends RichMapFunction<Event, Event> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<String> valueState;

        @Override
        public void open(Configuration parameters) {
            int index = getRuntimeContext().getIndexOfThisSubtask();
            valueState =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "valueState" + index, StringSerializer.INSTANCE));
        }

        @Override
        public Event map(Event event) throws Exception {
            valueState.update(event.getPayload());
            return event;
        }
    }
}
