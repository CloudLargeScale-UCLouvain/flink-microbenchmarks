package org.apache.flink.streaming.tests.queries;

import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.tests.sinks.DummySink;
import org.apache.flink.streaming.tests.sources.*;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class Query8 {

    private static final Logger logger  = LoggerFactory.getLogger(Query8.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        if (params.getBoolean("disabledOperatorChaining", false)) {
            env.disableOperatorChaining();
        }
        // enable latency tracking
        //env.getConfig().setLatencyTrackingInterval(5000);

        final int auctionSrcRate = params.getInt("auction-srcRate", 50000);

        final int personSrcRate = params.getInt("person-srcRate", 30000);

        env.setParallelism(params.getInt("p-window", 1));

        boolean stateless = params.getBoolean("stateless", false);

        logger.debug(String.valueOf(params.getInt("auction-srcRate")));

        DataStream<Person> persons = env.addSource(
                        stateless
                                ? new PersonStatelessSourceFunction(personSrcRate)
                                : new PersonSourceFunction(params))
                .name("Custom Source: Persons")
                .setParallelism(params.getInt("p-person-source", 1))
                .assignTimestampsAndWatermarks(new PersonTimestampAssigner());

        DataStream<Auction> auctions = env.addSource(
                        stateless
                                ? new AuctionStatelessSourceFunction(auctionSrcRate)
                                : new AuctionSourceFunction(params))
                .name("Custom Source: Auctions")
                .setParallelism(params.getInt("p-auction-source", 1))
                .assignTimestampsAndWatermarks(new AuctionTimestampAssigner());

        // SELECT Rstream(P.id, P.name, A.reserve)
        // FROM Person [RANGE 1 HOUR] P, Auction [RANGE 1 HOUR] A
        // WHERE P.id = A.seller;
        DataStream<Tuple3<Long, String, Long>> joined =
                persons.join(auctions)
                        .where(new KeySelector<Person, Long>() {
                            @Override
                            public Long getKey(Person p) {
                                return p.id;
                            }
                        }).equalTo(new KeySelector<Auction, Long>() {
                            @Override
                            public Long getKey(Auction a) {
                                return a.seller;
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Time.seconds(params.getInt("window-size", 10))))
                        .apply(new FlatJoinFunction<Person, Auction, Tuple3<Long, String, Long>>() {
                            @Override
                            public void join(Person p, Auction a, Collector<Tuple3<Long, String, Long>> out) {
                                out.collect(new Tuple3<>(p.id, p.name, a.reserve));
                            }
                        });


        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        joined.transform("DummyLatencySink", objectTypeInfo, new DummySink<>())
                .setParallelism(params.getInt("p-window", 1));

        // execute program
        env.execute("Nexmark Query8");
    }

    private static final class PersonTimestampAssigner implements AssignerWithPeriodicWatermarks<Person> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Person element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

    private static final class AuctionTimestampAssigner implements AssignerWithPeriodicWatermarks<Auction> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Auction element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, element.dateTime);
            return element.dateTime;
        }
    }

}