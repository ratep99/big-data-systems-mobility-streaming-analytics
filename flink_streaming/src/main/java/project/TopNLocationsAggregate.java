package project;

import models.Vehicle;
import models.Vehicle;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
public class TopNLocationsAggregate implements AggregateFunction<Vehicle, HashMap<String, Integer>, Tuple1<String>> {
    private final int n;
    public TopNLocationsAggregate(int n) {
        this.n = n;
    }
    @Override
    public HashMap<String, Integer> createAccumulator()
    {

        return new HashMap<>();
    }
    @Override
    public HashMap<String, Integer> add(Vehicle vehicle, HashMap<String, Integer> accumulator) {
        if(vehicle.getLatitudeRounded(2) != null && vehicle.getLongitudeRounded(2) != null) {
            String key = Double.toString(vehicle.getLatitudeRounded(2)) + " "
                    + Double.toString(vehicle.getLongitudeRounded(2));
            accumulator.merge(key, 1, Integer::sum);
        }
        return accumulator;
    }
    @Override
    public Tuple1<String> getResult(HashMap<String, Integer> accumulator) {
        List<Tuple2<String, Integer>> topLocations = new ArrayList<>();
        Comparator<Tuple2<String, Integer>> comparator = new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {return t2.f1.compareTo(t1.f1);}
        };
        for (Map.Entry<String, Integer> entry : accumulator.entrySet()) {
            if(entry.getKey() != null && entry.getValue() != null)
                topLocations.add(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));
        }
        Collections.sort(topLocations, comparator);
        if(n>=topLocations.size()){
            String output = "[";
            for(Tuple2<String,Integer> t : topLocations) {
                output += "(" + t.f0 + ", " + Integer.toString(t.f1) + "), ";
            }
            output+= "]";
            return new Tuple1<String>(output);
        }
        else{
            List<Tuple2<String, Integer>> topLocations_copy = new ArrayList<>();
            for(int i=0; i<n; i++) {
                topLocations_copy.add(topLocations.get(i));
            }
            String output = "[";
            for(Tuple2<String,Integer> t : topLocations_copy) {
                output += "(" + t.f0 + ", " + Integer.toString(t.f1) + "), ";
            }
            output+= "]";
            return new Tuple1<String>(output);
        }
    }
    @Override
    public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
        for (Map.Entry<String, Integer> entry : b.entrySet()) {
            a.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
        return a;
    }
}
