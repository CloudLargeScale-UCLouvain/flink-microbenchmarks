package be.uclouvain.gepiciad.micro;

import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.tests.SequenceGeneratorSource;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final ParameterTool pt = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.addSource(new RandomStringSource( pt))
                .keyBy(value -> value.f0)
                .sum(1)
                .addSink(new DiscardingSink<>());
        env.execute("WordCount");
    }


    public static class RandomStringSource extends RichParallelSourceFunction<Tuple2<String,Integer>>
            implements CheckpointedFunction {
        private int elementsPerSecond;
        private int sleepBatchSize;
        private final int sleepBatchTime;
        private final int changingRateInterval;
        private final double changingRateRatio;
        private final int changingRateSteps;
        private long lastChangingRate = 0;

        private double changingRateValue;
        private boolean increasingRate = true;
        private int steps = 1;
        private final int stringSize;

        private volatile boolean running = true;

        private transient ListState<Boolean> increasingRateState;
        private transient ListState<Integer> stepsState;
        private transient ListState<Integer> rateState;
        private transient ListState<Long> lastChangingRateState;

        RandomStringSource(ParameterTool pt) {


            this.stringSize = pt.getInt("stringSize", 7);
            this.elementsPerSecond = pt.getInt("elementsPerSecond", 10000);
            if (elementsPerSecond >= 100) {
                // how many elements would we emit per 50ms
                this.sleepBatchSize = elementsPerSecond / 20;
                this.sleepBatchTime = 50;
            } else if (elementsPerSecond >= 1) {
                // how long does element take
                this.sleepBatchSize = 1;
                this.sleepBatchTime = 1000 / elementsPerSecond;
            } else {
                this.sleepBatchSize = 1;
                this.sleepBatchTime = 1;
            }
            this.changingRateInterval = pt.getInt("changingRateInterval", 10000);
            this.changingRateRatio = pt.getDouble("changingRateRatio", 0.4);
            this.changingRateValue = this.elementsPerSecond * this.changingRateRatio;
            this.changingRateSteps = pt.getInt("changingRateSteps", 4);
        }

        @Override
        public void run(SourceContext<Tuple2<String,Integer>> sourceContext) throws Exception {
            long lastBatchCheckTime = 0;

            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            RandomStringGenerator rng = new RandomStringGenerator.Builder()
                    .withinRange('a', 'z')
                    .usingRandom(rnd::nextInt)
                    .build();
            long num = 0;
            if (lastChangingRate == 0) {
                lastChangingRate = System.currentTimeMillis();
            }

            while (running) {
                synchronized (sourceContext.getCheckpointLock()) {
                    String event = rng.generate(stringSize);
                    if (elementsPerSecond > 0) {
                        if (lastBatchCheckTime > 0) {
                            if (++num >= sleepBatchSize) {
                                num = 0;

                                final long now = System.currentTimeMillis();
                                final long elapsed = now - lastBatchCheckTime;
                                if (elapsed < sleepBatchTime) {
                                    try {
                                        Thread.sleep(sleepBatchTime - elapsed);
                                    } catch (InterruptedException e) {
                                        // restore interrupt flag and proceed
                                        Thread.currentThread().interrupt();
                                    }
                                }
                                lastBatchCheckTime = now;
                            }
                        } else {
                            lastBatchCheckTime = System.currentTimeMillis();
                        }
                    }
                    sourceContext.collect(Tuple2.of(event, 1));
                    long endTime = System.currentTimeMillis();
                    if (endTime - lastChangingRate > this.changingRateInterval) {
                        if (increasingRate) {
                            elementsPerSecond += changingRateValue;
                            LoggerFactory.getLogger(SequenceGeneratorSource.class).info("SequenceGenerator: Increasing rate to {}", elementsPerSecond);
                        } else {
                            elementsPerSecond -= changingRateValue;
                            LoggerFactory.getLogger(SequenceGeneratorSource.class).info("SequenceGenerator: Decreasing rate to {}", elementsPerSecond);
                        }
                        this.sleepBatchSize = elementsPerSecond / 20;

                        if (steps % changingRateSteps == 0) {
                            increasingRate = !increasingRate;
                        }
                        lastChangingRate = endTime;
                        steps++;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            increasingRateState.clear();
            increasingRateState.add(increasingRate);

            stepsState.clear();
            stepsState.add(steps);

            rateState.clear();
            rateState.add(elementsPerSecond);

            lastChangingRateState.clear();
            lastChangingRateState.add(lastChangingRate);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            final RuntimeContext runtimeContext = getRuntimeContext();

            ListStateDescriptor<Long> unionWatermarksStateDescriptor =
                    new ListStateDescriptor<>("watermarks", Long.class);

            increasingRateState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Boolean>("increasingRateState", BooleanSerializer.INSTANCE)
            );
            stepsState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Integer>("stepsState", IntSerializer.INSTANCE)
            );
            rateState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Integer>("rateState", IntSerializer.INSTANCE)
            );
            lastChangingRateState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("lastChangingRateState", LongSerializer.INSTANCE)
            );

            if (context.isRestored()) {

                for (Boolean b : increasingRateState.get()) {
                    increasingRate = b;
                }
                for (Integer i : stepsState.get()) {
                    steps = i;
                }
                for (Integer d : rateState.get()) {
                    elementsPerSecond = d;
                    this.sleepBatchSize = elementsPerSecond / 20;
                }
                for (Long d : lastChangingRateState.get()) {
                    lastChangingRate = d;
                }
            }
        }
    }
}
