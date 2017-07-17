/*
 * 项目名：beifeng-spark
 * 文件名：WordCount.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：基于socket的实时单词统计
 * 修改人：yanglin
 * 修改时间：2016年11月21日 上午10:17:11
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * WordCount
 *	
 * @Description 基于socket的实时单词统计
 * @author yanglin
 * @version 1.0,2016年11月21日
 * @see
 * @since
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("WordCount").setMaster("local[2]");
        //初始化javaStreamingContext对象
        JavaStreamingContext streamingContext=new JavaStreamingContext(conf, 
                Durations.seconds(1)//设置多长时间为一个批次
        );
        streamingContext.checkpoint("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/streaming/checkpoint");
        //streamingContext.checkpoint("src/main/resources/checkpoint");
        String hostName="hadoop-senior.ibeifeng.com";
        //String hostName="hadoop-senior.ibeifeng.com";
        //String hostName="127.0.0.1";
        Integer port=9999;
        //监听主机名为hadoop-senior.ibeifeng.com的9999端口号
        JavaPairDStream<String, Integer> pairs=streamingContext.socketTextStream(hostName, port)
            .flatMap(new FlatMapFunction<String, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<String> call(String str) throws Exception {
                    return Arrays.asList(str.split(" "));
                }
            })
            .mapToPair(new PairFunction<String, String, Integer>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Integer> call(String word) throws Exception {
                    return new Tuple2<String, Integer>(word, 1);
                }
            });
            /*//统计当前批次的单词计数
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                
                private static final long serialVersionUID = 1L;

                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1+v2;
                }
            })*/
        
            //基于updateStateByKey算子实现缓存机制的实时wordcount程序
            pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

                private static final long serialVersionUID = 1L;

                /**
                 * 第一个参数为：相当于是这个batch中，这个key的新的值
                 * 第二个参数为：指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
                 */
                @Override
                public Optional<Integer> call(List<Integer> values, Optional<Integer> states)
                        throws Exception {
                    //首先定义一个全局的单词计数
                    Integer newValue=0;
                    
                    //判断这个单词是否是首次出现，如果不是获取已有的个数
                    if (states.isPresent()) {
                        newValue=states.get();
                    }
                    
                    //将本次出现的次数添加到原来的次数中
                    for(Integer value:values){
                        newValue+=value;
                    }
                    return Optional.of(newValue); 
                }
            }).print();
            //.print();
        
        
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }

}
