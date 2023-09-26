package project;

import models.Vehicle;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class TopNLocationsAggregate implements AggregateFunction<Vehicle, Map<String, Integer>, Tuple1<String>> {
    private final int n;
    public TopNLocationsAggregate(int n) {
        this.n = n;
    }
    @Override
    public Map<String, Integer> createAccumulator() {
        return new HashMap<>();
    }
    @Override
    public Map<String, Integer> add(Vehicle vehicle, Map<String, Integer> accumulator) {
        double roundedLatitude = roundToTwoDecimals(vehicle.getLatitude());
        double roundedLongitude = roundToTwoDecimals(vehicle.getLongitude());

        String key = roundedLatitude + " " + roundedLongitude;
        accumulator.merge(key, 1, Integer::sum);
        return accumulator;
    }
    @Override
    public Tuple1<String> getResult(Map<String, Integer> accumulator) {
        List<Tuple2<String, Integer>> sortedLocations = accumulator.entrySet().stream()
                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .sorted((e1, e2) -> e2.f1.compareTo(e1.f1))
                .collect(Collectors.toList());

        int size = Math.min(n, sortedLocations.size());
        List<Tuple2<String, Integer>> topLocations = sortedLocations.subList(0, size);

        String output = topLocations.toString();
        return new Tuple1<>(output);
    }
    @Override
    public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {
        b.forEach((key, value) -> a.merge(key, value, Integer::sum));
        return a;
    }

    private double roundToTwoDecimals(Double value) {
        if (value == null) return 0.0;
        return Math.round(value * 100.0) / 100.0;
    }
}
