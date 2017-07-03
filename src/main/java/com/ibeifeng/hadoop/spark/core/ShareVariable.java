/*
 * 项目名：beifeng-spark
 * 文件名：ShareVariable.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：共享变量
 * 修改人：yanglin
 * 修改时间：2016年11月7日 下午5:09:02
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * ShareVariable
 *	
 * @Description 共享变量
 *      spark算子中使用到了外部的某个变量，那么这个变量会被拷贝到各个task中，
 *          如果多个task想要共享某个变量，是做不到的，此时需要使用共享变量
 *      spark提供了两种共享变量：Broadcast variable(广播变量) 和 Accumuatro (累积变量)
 *          共享变量仅仅为每个节点拷贝一份
 *          Broadcast variable(广播变量):共享只读
 *          Accumuatro (累积变量):共享可写
 * @author yanglin
 * @version 1.0,2016年11月7日
 * @see
 * @since
 */
public class ShareVariable {
    
    public static void main(String[] args) {
        JavaSparkContext context=TransactionOperation.getSparkContext();
        broadCastVariable(context);
        //accumulatorVariable(context);
        TransactionOperation.closeSparkContext(context);
    }
    
    /**
     * 
     * broadCastVariable
     * 
     * @Description 广播变量的使用
     * @param context
     * @return void 
     * @see
     * @since
     */
    public static void broadCastVariable(JavaSparkContext context) {
        Integer factor=5;
        //广播变量的定义
        final Broadcast<Integer> broadcast=context.broadcast(factor);
        List<Integer> list=new ArrayList<Integer>();
        final Broadcast<List<Integer>> broadcast2 = context.broadcast(list);
        context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10))
        .map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            public Integer call(Integer v) throws Exception {
                broadcast2.getValue().add(v);
                //广播变量的获取
                return v*broadcast.getValue();
            }
        }).foreach(new VoidFunction<Integer>() {
            
            private static final long serialVersionUID = 1L;

            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });
        
        System.out.println(broadcast2.getValue());
    }
    
    /**
     * 
     * accumulatorVariable
     * 
     * @Description 累加变量的使用
     * @param context
     * @return void 
     * @see
     * @since
     */
    public static void accumulatorVariable(JavaSparkContext context) {
        Integer factor=0;
        //定义累加变量
        final Accumulator<Integer> accumulator=context.accumulator(factor);
        context.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10))
        .foreach(new VoidFunction<Integer>() {

            private static final long serialVersionUID = 1L;

            public void call(Integer v) throws Exception {
                accumulator.add(v);
            }
        });
        //获取累加变量
        System.out.println(accumulator.value());
    }
    
    
}
