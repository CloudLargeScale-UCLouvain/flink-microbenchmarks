/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;
import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_SOURCE;

/**
 * The test program for a job that simply accumulates data in various states. This is used to stress
 * the RocksDB memory and check that the cache/write buffer management work properly, limiting the
 * overall memory footprint of RocksDB.
 */
public class LatencyComparison {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);
        final boolean useValueState = pt.getBoolean("useValueState", false);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        setupEnvironment(env, pt);
        KeyedStream<Event, Integer> keyedStream =
                env.addSource(DataStreamAllroundTestJobFactory.createEventSource(pt))
                        .name(EVENT_SOURCE.getName())
                        .uid(EVENT_SOURCE.getUid())
                        .keyBy(Event::getKey);

        keyedStream
                .map(new ValueStateMapper(pt))
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

        private final ParameterTool params;

        public ValueStateMapper(ParameterTool params) {
            this.params = params;
        }

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
            String value = valueState.value();
            int res = 0;
            for (int i = 0; i < params.getInt("iterations", 500); i++) {
                res += Math.sin(StrictMath.cos(res)) * 2;
            }
            if (value != null) {
                return event;
            }

            valueState.update(event.getPayload());
            return event;
        }
    }
}
