package org.bf.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class TupleDemo {

    public static void main(String[] args) {
        Tuple2<String, Integer> person = new Tuple2<>("xiaoping", 18);
        System.out.println(person.f0);
        System.out.println(person.f1);

        Tuple3<String, String, Integer> emp = new Tuple3<>("jack", "it", 50000);
        System.out.println(emp.f0);
        System.out.println(emp.f1);
        System.out.println(emp.f2);
    }

}
