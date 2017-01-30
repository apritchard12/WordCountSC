package org;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class CompareWords implements Comparator<Tuple2<Integer, String>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, String> tuple1, Tuple2<Integer, String> tuple2) {
        return tuple1._1() - tuple2._1();
    }
}