package project;

import models.Vehicle;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class AverageAggregate implements AggregateFunction<Vehicle, Tuple5<String, Double, Double, Double, Integer>,
        Tuple5<String, Double, Double, Double, Integer>> {

    @Override
    public Tuple5<String, Double, Double, Double, Integer> createAccumulator() {
        return new Tuple5<>("", 0.0, Double.MAX_VALUE, 0.0, 0);
    }

    @Override
    public Tuple5<String, Double, Double, Double, Integer> add(Vehicle value, Tuple5<String, Double, Double, Double, Integer> accumulator)
    {
        String id = value.getId();
        Double speed = value.getSpeed_kmh();
        Double minSpeed = Math.min(value.getSpeed_kmh(), accumulator.f2);
        Double maxSpeed = Math.max(value.getSpeed_kmh(), accumulator.f3);
        Integer count = accumulator.f4 + 1;

        return new Tuple5<>(id, accumulator.f1 + speed, minSpeed, maxSpeed, count);
    }


    @Override
    public Tuple5<String, Double, Double, Double, Integer> getResult(Tuple5<String, Double, Double, Double, Integer> acc) {
        return new Tuple5<>(acc.f0, acc.f2, acc.f3, acc.f1 / acc.f4, acc.f4);
    }
    @Override
    public Tuple5<String, Double, Double, Double, Integer> merge(Tuple5<String, Double, Double, Double, Integer> acc1,
                                                                Tuple5<String, Double, Double, Double, Integer> acc2)
    {
        return new Tuple5<>(acc1.f0, acc1.f1 + acc2.f1,
                acc1.f2 < acc2.f2 ? acc1.f2 : acc2.f2, acc1.f2 > acc2.f2 ? acc1.f2 : acc2.f2, acc1.f4 + acc2.f4);
    }
}
