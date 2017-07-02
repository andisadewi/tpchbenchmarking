package de.tuberlin.dima.bdapro.flink.tpch.streaming;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple10;

/**
 * Created by seema on 01/07/2017.
 */
public class orderByFunction implements java.util.Comparator<Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer>> {
    @Override
    public int compare(Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> a,
                       Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> b) {
        int compare = (a.f0).compareTo(b.f0);
        if (compare < 0){
            return -1;
        }
        if (a.f0 == b.f1) {
            return 0;
        }
        return 1;
    }
}
