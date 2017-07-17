/*
 * 项目名：beifeng-spark
 * 文件名：TransformAdClickLog.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：基于transform实现黑名单的广告日志实时过滤
 * 修改人：yanglin
 * 修改时间：2017年7月16日 上午8:04:53
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.HiveParser.resourceType_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * TransformAdClickLog
 *	
 * @Description 基于transform实现黑名单的广告日志实时过滤
 * @author yanglin
 * @version 1.0,2017年7月16日
 * @see
 * @since
 */
public class TransformAdClickLog {

    
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TransformAdClickLog").setMaster("local[2]");
        JavaStreamingContext ssc=new JavaStreamingContext(conf,Durations.seconds(5));
        List<Tuple2<String, Boolean>> blackUserList=new ArrayList<Tuple2<String, Boolean>>();
        blackUserList.add(new Tuple2<String, Boolean>("tom", true));
        final JavaPairRDD<String, Boolean> blackUserRDD= ssc.sparkContext().parallelizePairs(blackUserList);
        //日志格式为：date log
        ssc.socketTextStream("hadoop-senior.ibeifeng.com", 9999)
            .mapToPair(log->new Tuple2<>(log.split(" ")[1], log))
            /**
             * transform : 对Dstream中各个batch中RDD进行算子操作
             *      使用了jdk8的lambda表达式代替匿名内部类的使用
             * 
             */
            .transform(userLogRDD->{
                JavaRDD<String> filteredClickLog=userLogRDD.leftOuterJoin(blackUserRDD)
                        //黑名单过滤
                        .filter(userLogJoinedRdd->!(userLogJoinedRdd._2._2().isPresent()&&userLogJoinedRdd._2._2().get()))
                        .map(userLogJoinedRdd->userLogJoinedRdd._2._1);
                return filteredClickLog;})
            /*.transform(new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

                private static final long serialVersionUID = 1L;

                @Override
                public JavaRDD<String> call(JavaPairRDD<String, String> userLogRDD)
                        throws Exception {
                    JavaRDD<String> filteredClickLog=userLogRDD.leftOuterJoin(blackUserRDD)
                        .filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Boolean call(
                                    Tuple2<String, Tuple2<String, Optional<Boolean>>> userLogJoinedRdd)
                                    throws Exception {
                                if(userLogJoinedRdd._2._2().isPresent()&&userLogJoinedRdd._2._2().get()){
                                    return false;
                                }
                                
                                return true;
                            }
                        })
                        .map(userLogJoinedRdd->userLogJoinedRdd._2._1);
                    return filteredClickLog;
                }
            })*/.print()
            ;
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }

}
