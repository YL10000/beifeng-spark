/*
 * 项目名：beifeng-spark
 * 文件名：UpdateStateByKeyWordCount.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：基于updateByKey来实现缓存中的单词计数
 * 修改人：yanglin
 * 修改时间：2017年7月16日 上午7:20:00
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * UpdateStateByKeyWordCount
 *	
 * @Description 基于updateByKey来实现缓存中的单词计数
 * @author yanglin
 * @version 1.0,2017年7月16日
 * @see
 * @since
 */
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local[2]");
        JavaStreamingContext ssc=new JavaStreamingContext(conf,Durations.seconds(5));
        /**
         * 如果要使用updateStateByKey方法，必须设置checkpoint目录
         * 各个可以的中间状态会被保存的checkpoint目录中
         */
        ssc.checkpoint("hdfs://hadoop-senior.ibeifeng.com:8020/user/yanglin/spark/streaming/checkpoint");
        ssc.socketTextStream("hadoop-senior.ibeifeng.com", 9999)
        .flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        }).updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state)
                    throws Exception {
                Integer newState=0;
                //判断指定的key是否存在
                if (state.isPresent()) {
                    newState=state.get();
                }
                
                //在原有的基础上，添加本次batch出现的该key对应的个数
                if (values!=null&&values.size()>0) {
                    for(Integer v:values){
                        newState+=v;
                    }
                }
                //将修改的的state返回
                return Optional.of(newState);
            }
        }).foreachRDD(new Function<JavaPairRDD<String,Integer>, Void>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountRDD) throws Exception {
                
                //对每个批次中的RDD,针对不同的分区进行计算，让每个分区公用一个Connection
                wordCountRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
                    
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        //从静态连接池中获取mysql连接
                        Connection connection=ConnectionPool.getConnection();
                        Tuple2<String, Integer> wordCount=null;
                        while(wordCounts.hasNext()){
                            wordCount=wordCounts.next();
                            String sql="insert into wordcount(word,count) values('"+wordCount._1+"',"+wordCount._2()+")";
                            Statement createStatement = connection.createStatement();
                            createStatement.executeUpdate(sql);
                        }
                        //将本次使用的连接放回到静态连接池中
                        ConnectionPool.returnConnection(connection);
                    }
                });
                return null;
            }
        });
        
        
        //.print()
        ;
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
