/*
 * 项目名：beifeng-spark
 * 文件名：HdfsStreaming.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：监听hdfs文件系统的目录
 * 修改人：yanglin
 * 修改时间：2016年11月21日 上午11:49:59
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;

import scala.Tuple2;

/**
 * HdfsStreaming
 *	
 * @Description 监听hdfs文件系统的目录
 * @author yanglin
 * @version 1.0,2016年11月21日
 * @see
 * @since
 */
public class HdfsStreaming {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("HdfsStreaming").setMaster("local[2]");
        JavaStreamingContext streamingContext=new JavaStreamingContext(conf, Durations.seconds(2));
        JavaDStream<String> dStream=streamingContext
                .textFileStream("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/streaming/hdfs");
        dStream.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).print();  
        
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }

}
