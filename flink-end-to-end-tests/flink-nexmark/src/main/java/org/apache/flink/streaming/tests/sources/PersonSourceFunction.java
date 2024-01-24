package org.apache.flink.streaming.tests.sources;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

/**
 * A ParallelSourceFunction that generates Nexmark Person data
 */
public class PersonSourceFunction extends RichParallelSourceFunction<Person> implements CheckpointedFunction {

    private volatile boolean running = true;
    private final GeneratorConfig config = new GeneratorConfig(NexmarkConfiguration.DEFAULT, 1, 1000L, 0, 1);
    private long eventsCountSoFar = 0;
    private final long eventsEmitted;
    private double rate;

    private final boolean fixedRate;
    private final int changingRateInterval;
    private final double changingRateRatio;
    private final int changingRateSteps;
    private long lastChangingRate = 0;

    private double changingRateValue;

    private boolean increasingRate = true;
    private int steps = 1;

    private ListState<Long> eventsCountSoFarState;
    private ListState<Boolean> increasingRateState;
    private ListState<Integer> stepsState;
    private ListState<Double> rateState;
    private ListState<Long> lastChangingRateState;

    public PersonSourceFunction(int srcRate) {
        this.rate = srcRate;
        this.eventsEmitted = 40_000_000;
        this.fixedRate = true;
        this.changingRateInterval = 300000;
        this.changingRateRatio = 1.5;
        this.changingRateSteps = 5;
    }

    public PersonSourceFunction(ParameterTool params) {
        this.rate = params.getDouble("person-srcRate", 30000);
        this.eventsEmitted = params.getLong("person-eventsEmitted", Long.MAX_VALUE);
        this.fixedRate = params.getBoolean("fixedRate", true);
        this.changingRateInterval = params.getInt("changingRateInterval", 300000); // In ms
        this.changingRateRatio = params.getDouble("changingRateRatio", 1.5);
        this.changingRateValue = this.rate * this.changingRateRatio;
        this.changingRateSteps = params.getInt("changingRateSteps", 5);
        LoggerFactory.getLogger(PersonSourceFunction.class).info(this.toString());
    }

    @Override
    public void run(SourceContext<Person> ctx) throws Exception {
        if (lastChangingRate == 0) {
            lastChangingRate = System.currentTimeMillis();
        }
        while (running && eventsCountSoFar < eventsEmitted) {
            long emitStartTime = System.currentTimeMillis();

            for (int i = 0; i < rate; i++) {
                long nextId = nextId();
                Random rnd = new Random(nextId);

                // When, in event time, we should generate the event. Monotonic.
                long eventTimestamp =
                        config.timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumber(eventsCountSoFar)).getKey();

                ctx.collect(PersonGenerator.nextPerson(nextId, rnd, eventTimestamp, config));
                eventsCountSoFar++;
            }
            long endTime = System.currentTimeMillis();
            if (!fixedRate && endTime - lastChangingRate > this.changingRateInterval) {
                if (increasingRate) {
                    rate += changingRateValue;
                    LoggerFactory.getLogger(PersonSourceFunction.class).info("Person: Increasing rate to {}", rate);
                } else {
                    rate -= changingRateValue;
                    LoggerFactory.getLogger(PersonSourceFunction.class).info("Person: Decreasing rate to {}", rate);
                }
                if (steps % changingRateSteps == 0) {
                    increasingRate = !increasingRate;
                }
                lastChangingRate = endTime;
                steps++;
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = endTime - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("PersonRate", new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return rate;
                    }
                });

    }

    private long nextId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        eventsCountSoFarState.update(Collections.singletonList(eventsCountSoFar));
        increasingRateState.update(Collections.singletonList(increasingRate));
        stepsState.update(Collections.singletonList(steps));
        rateState.update(Collections.singletonList(rate));
        lastChangingRateState.update(Collections.singletonList(lastChangingRate));
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        eventsCountSoFarState = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<Long>("eventsCountSoFarState", LongSerializer.INSTANCE)
        );
        increasingRateState = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<Boolean>("increasingRateState", BooleanSerializer.INSTANCE)
        );
        stepsState = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<Integer>("stepsState", IntSerializer.INSTANCE)
        );
        rateState = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<Double>("rateState", DoubleSerializer.INSTANCE)
        );
        lastChangingRateState = functionInitializationContext.getOperatorStateStore().getListState(
                new ListStateDescriptor<Long>("lastChangingRateState", LongSerializer.INSTANCE)
        );

        for (Long l : eventsCountSoFarState.get()) {
            eventsCountSoFar = l;
        }
        for (Boolean b: increasingRateState.get()){
            increasingRate = b;
        }
        for (Integer i: stepsState.get()){
            steps = i;
        }
        for (Double d: rateState.get()){
            rate = d;
        }
        for (Long d: lastChangingRateState.get()){
            lastChangingRate = d;
        }
    }

    @Override
    public String toString() {
        return "PersonSourceFunction{" +
                "eventsCountSoFar=" + eventsCountSoFar +
                ", eventsEmitted=" + eventsEmitted +
                ", rate=" + rate +
                ", fixedRate=" + fixedRate +
                ", changingRateInterval=" + changingRateInterval +
                ", changingRateRatio=" + changingRateRatio +
                ", changingRateSteps=" + changingRateSteps +
                ", changingRateValue=" + changingRateValue +
                ", increasingRate=" + increasingRate +
                ", steps=" + steps +
                '}';
    }
}