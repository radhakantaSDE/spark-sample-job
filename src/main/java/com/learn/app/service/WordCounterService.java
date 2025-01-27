package com.learn.app.service;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class WordCounterService {

    @Autowired
    JavaSparkContext sc;

    public Map<String, Integer> getCount(List<String> wordList) {

        JavaRDD<String> words = sc.parallelize(wordList);

        // sum words
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                w -> new Tuple2<String, Integer>(w, 1)).reduceByKey(
                Integer::sum);

        JavaPairRDD<Tuple2<Integer, String>, Integer> countInKey = counts.mapToPair(a -> new Tuple2(new Tuple2<Integer, String>(a._2, a._1), null));

        JavaPairRDD<Tuple2<Integer, String>, Integer> wordSortedByCount = countInKey.sortByKey(new TupleComparator(), false);

        // print result
        Map<String, Integer> res = new LinkedHashMap<>();
        List<Tuple2<Tuple2<Integer, String>, Integer>> output = wordSortedByCount.take(100);
        for (Tuple2<Tuple2<Integer, String>, Integer> tuple : output) {

            res.put(tuple._1()._2(), tuple._1()._1());
        }
        return res;
    }
}
