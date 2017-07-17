/*
 * 项目名：beifeng-spark
 * 文件名：KafkaDirectStreaming.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：基于kakfa的Direct 方式的实时单词统计
 * 修改人：yanglin
 * 修改时间：2017年7月15日 下午4:33:24
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


/**
 * KafkaDirectStreaming
 *	
 * @Description 基于kakfa的Direct 方式的实时单词统计 
 *      实现原理：定期从Kafka的topic+partition中查询最新的偏移量，在根据定义的偏移量范围在每个batch中处理数据。
 *      与Receivers方式的区别：
 *          优点：
 *              1.简单并行：不需要创建多个Kafka输入流。
 *                      spark streaming 会创建和kafka分区一样的RDD分区个数，并且从Kafka中并行地读取数据，
 *                      即：spark分区和kafka分区有一一对应的关系
 *              2.高效：不需要写入wal日志
 *              3.保证了消息仅且消费一次：
 *                  receivers:通过Kafka高层次api把偏移量写入到zookeeper中，
 *                      在失败的情况下，spark streaming中的偏移量和zookeeper中的偏移量不一致
 *                  direct：通过kakfa底层api，偏移量仅被保存到了spark streaming的checkpoint目录下
 *          缺点：
 *              没有更新zookeeper中的偏移量，所以基于zookeeper的kafka监控工具将无法显示消费的情况。
 *              可以通过spark的api手动将偏移量写入到zookeeper中
 * @author yanglin
 * @version 1.0,2017年7月15日
 * @see
 * @since
 */
public class KafkaDirectStreaming {

    public static void main(String[] args) {
        String topic="helloword";
        String broker_list="hadoop-senior.ibeifeng.com:9092";
        SparkConf conf=new SparkConf().setAppName("KafkaDirectStreaming").setMaster("local[2]");
        JavaStreamingContext ssc=new JavaStreamingContext(conf, Durations.seconds(5));
        Map<String, String> kakfaParams=new HashMap<String, String>();
        kakfaParams.put("metadata.broker.list", broker_list);
        Set<String> topics=new HashSet<String>();
        topics.add(topic);
        JavaPairInputDStream<String, String> inputDString=KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kakfaParams, topics);
        inputDString.map(new Function<Tuple2<String,String>, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> tuple) throws Exception {
                return tuple._2;
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
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
        
               
    }
}
