/*
 * 项目名：beifeng-spark
 * 文件名：TransactionOperation.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：transaction操作
 * 修改人：yanglin
 * 修改时间：2016年11月7日 下午3:08:07
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * TransactionOperation
 * 
 * @Description transaction操作
 * @author yanglin
 * @version 1.0,2016年11月7日
 * @see
 * @since
 */
@SuppressWarnings("unchecked")
public class TransactionOperation {

    public static void main(String[] args) {
        JavaSparkContext context = getSparkContext();
        // filterAndReduce(context);
        joinAndCogroup(context);
        closeSparkContext(context);
    }

    /**
     * 
     * getSparkContext
     * 
     * @Description 获取javaSparkContext对象
     * @return
     * @return JavaSparkContext
     * @see
     * @since
     */
    public static JavaSparkContext getSparkContext() {
        SparkConf sparkConf = new SparkConf().setAppName("TransactionOperation")
                .setMaster("local");
        return new JavaSparkContext(sparkConf);
    }

    /**
     * 
     * closeSparkContext
     * 
     * @Description 关闭javaSparkContext对象
     * @param sparkContext
     * @return void
     * @see
     * @since
     */
    public static void closeSparkContext(JavaSparkContext sparkContext) {
        sparkContext.close();
    }

    /**
     * 
     * filterAndReduce
     * 
     * @Description filter操作
     * @param context
     * @return void
     * @see
     * @since
     */
    public static void filterAndReduce(JavaSparkContext context) {
        Integer evenSum = context
                .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                //过滤如何条件的数据
                .filter(new Function<Integer, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    public Boolean call(Integer val) throws Exception {
                        return val % 2 == 0;
                    }
                }).reduce(new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        System.out.println(evenSum);
    }

    /**
     * 
     * joinAndConJoin
     * 
     * @Description
     * 
     * @param context
     * @return void
     * @see
     * @since
     */
    public static void joinAndCogroup(JavaSparkContext context) {
        JavaPairRDD<Integer, String> students = context.parallelizePairs(
                Arrays.asList(new Tuple2<Integer, String>(1, "zhangsan"),
                        new Tuple2<Integer, String>(2, "lisi"),
                        new Tuple2<Integer, String>(3, "wangwu"),
                        new Tuple2<Integer, String>(4, "zhaoliu"),
                        new Tuple2<Integer, String>(1, "lilang")),
                
                1);
        
        JavaPairRDD<Integer, Integer> scores = context.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(1, 69), new Tuple2<Integer, Integer>(1, 68),
                new Tuple2<Integer, Integer>(2, 35), new Tuple2<Integer, Integer>(2, 97),
                new Tuple2<Integer, Integer>(3, 48), new Tuple2<Integer, Integer>(3, 52),
                new Tuple2<Integer, Integer>(4, 21)));
        join(students, scores);
        //cogroup(students, scores);
    }

    /**
     * 
     * join 把相同的key进行关联组合
     * 
     * @Description join的结果为： 
     *      [(4,(zhaoliu,21)), (1,(zhangsan,69)), (1,(zhangsan,68)), (1,(lilang,69)), (1,(lilang,68)), (3,(wangwu,48)), (3,(wangwu,52)), (2,(lisi,35)), (2,(lisi,97))]
     * @return void
     * @see
     * @since
     */
    public static void join(JavaPairRDD<Integer, String> students,
            JavaPairRDD<Integer, Integer> scores) {
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScorse = students
                .join(scores);
        
        System.out.println(studentScorse.collect());
       
    }

    /**
     * 
     * cogroup
     * 
     * @Description 把相同的key进行关分组合并
     * cogroup的结果：
     *      [(4,([zhaoliu],[21])), (1,([zhangsan, lilang],[69, 68])), (3,([wangwu],[48, 52])), (2,([lisi],[35, 97]))]
     * @param students
     * @param scores
     * @return void 
     * @see
     * @since
     */
    public static void cogroup(JavaPairRDD<Integer, String> students,
            JavaPairRDD<Integer, Integer> scores) {
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScorse = students
                .cogroup(scores);
        System.out.println(studentScorse.collect());
    }
}
