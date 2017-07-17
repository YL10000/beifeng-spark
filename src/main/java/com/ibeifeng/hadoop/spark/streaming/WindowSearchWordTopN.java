/*
 * 项目名：beifeng-spark
 * 文件名：WindowSearchWordTopN.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述： 使用滑动窗口，每隔十秒统计前60秒的单词出现的频率倒序排列取前3
 * 修改人：yanglin
 * 修改时间：2017年7月16日 上午9:50:27
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.collection.parallel.ParIterable;

/**
 * WindowSearchWordTopN
 *	
 * @Description 使用滑动窗口，每隔十秒统计前60秒的单词出现的频率倒序排列取前3
 * @author yanglin
 * @version 1.0,2017年7月16日
 * @see
 * @since
 */
public class WindowSearchWordTopN {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("WindowSearchWordTopN").setMaster("local[2]");
        JavaStreamingContext ssc=new JavaStreamingContext(conf, Durations.seconds(1));
        //搜索日志格式：user word
        ssc.socketTextStream("hadoop-senior.ibeifeng.com", 9999)
            .map(new Function<String, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public String call(String line) throws Exception {
                    return line.split(" ")[1];
                }
            }).mapToPair(new PairFunction<String, String, Integer>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Integer> call(String word) throws Exception {
                    return new Tuple2<String, Integer>(word, 1);
                }
            })
            //使用滑动窗口函数：计算每隔10s,前60s的单词统计
            //实现原理：每个10s，会把前60s的数据作为一个batch进行处理
            .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                
                private static final long serialVersionUID = 1L;

                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
            }, Durations.seconds(60),//窗口长度
               Durations.seconds(10)//窗口间隔
             ).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Integer, String> call(Tuple2<String, Integer> wordCount)
                        throws Exception {
                    return new Tuple2<Integer, String>(wordCount._2, wordCount._1);
                }
            })
            //对每个window中RDD进行排序取top3的操作
            .transformToPair(new Function<JavaPairRDD<Integer,String>, JavaPairRDD<String,Integer>>() {

                private static final long serialVersionUID = 1L;

                @Override
                public JavaPairRDD<String, Integer> call(JavaPairRDD<Integer, String> countWordRDD)
                        throws Exception {
                    JavaPairRDD<String, Integer> wordCountRDD=countWordRDD.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

                        private static final long serialVersionUID = 1L;

                        @Override
                        public Tuple2<String, Integer> call(Tuple2<Integer, String> countWordRDD)
                                throws Exception {
                            return new Tuple2<String, Integer>(countWordRDD._2, countWordRDD._1);
                        }
                    });
                    List<Tuple2<String, Integer>> topWordCountList=wordCountRDD.take(3);
                    System.out.println(topWordCountList);
                    for(Tuple2<String, Integer> tuple2:topWordCountList){
                        System.out.println(tuple2._1+"----"+tuple2._2());
                    }
                    return wordCountRDD;
                }
            }).print();
        
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
