/*
 * 项目名：beifeng-spark
 * 文件名：MergeSchema.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：合并元数据
 * 修改人：yanglin
 * 修改时间：2016年11月15日 下午7:49:41
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.collection.Seq;

/**
 * MergeSchema
 *	
 * @Description 合并元数据
 *      开启合并元数据的两种方式：
 *          1) sqlContext.read().option("mergeSchema", "true")
 *          2) SparkConf().set("spark.sql.parquet.mergeSchema", "true")
 * 
 * @author yanglin
 * @version 1.0,2016年11月15日
 * @see
 * @since
 */
public class MergeSchema {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("MergeSchema")
                //.set("spark.sql.parquet.mergeSchema", "true")
                .setMaster("local");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(javaSparkContext);
        
        /**
         * megerschema/idandage.txt 中的内容只有id和age两个属性
         * megerschema/idandname.txt 中的内容只有id和name两个属性
         * 合并以后的元素为id,name,age三个属性
         */
        //合并两个元数据
        DataFrame personDF=sqlContext.read().option("mergeSchema", "true").format("json").load("src/main/resources/megerschema");
        personDF.printSchema();
        personDF.show();
    }

}
