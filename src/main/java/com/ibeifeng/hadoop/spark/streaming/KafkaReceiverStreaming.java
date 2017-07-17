/*
 * 项目名：beifeng-spark
 * 文件名：KafkaReceiverStreaming.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：基于kakfa的receiver 方式的实时单词统计
 * 修改人：yanglin
 * 修改时间：2017年7月15日 上午10:28:58
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * KafkaReceiverStreaming
 *	
 * @Description 基于kakfa的receiver 方式的实时单词统计
 *      实现原理：使用Receivers来接收数据，Receivers实现使用到了Kafka高层次的消费者API。
 *              对于所有的Receivers接收到数据后，将保存到spark executors中，然后又spark Streaming启动的job来处理数据。
 *              默认情况这种方式在失败时会造成数据丢失，spark1.2.0之后引入了预写日志机制(将接收到的数据先写入到WAL中一份，失败之后从这里恢复)。
 * @author yanglin
 * @version 1.0,2017年7月15日
 * @see
 * @since
 */
public class KafkaReceiverStreaming {

    public static void main(String[] args) {
        String toptic="helloword";
        String zookeeper_url="hadoop-senior.ibeifeng.com:2181";
        /**
         * 设置topic的分区数：
         *      该分区数和RDD的分区数不是同一概念，这里的分区数只是指一个Receiver中消费Topic的线程数，并不增加spark并行处理数据的能力
         */
        Map<String, Integer> kafkaThread=new HashMap<String, Integer>();
        kafkaThread.put(toptic, 1);
        SparkConf conf=new SparkConf().setAppName("KafkaReceiverStreaming").setMaster("local[2]");
        JavaStreamingContext streamingContext=new JavaStreamingContext(conf,Durations.seconds(5));
        
        //对于不同的topic，我们可以使用多个receivers创建不同的DStreams来并行接受数据
        KafkaUtils.createStream(streamingContext, zookeeper_url, "DefaultConsumerGroup", kafkaThread)
            .map(new Function<Tuple2<String,String>, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public String call(Tuple2<String, String> tuple2) throws Exception {
                    return tuple2._2;
                }
            }).flatMap(new FlatMapFunction<String, String>() {

                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<String> call(String line) throws Exception {
                    return Arrays.asList(line.split(" "));
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
