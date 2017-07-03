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
 * @author yanglin
 * @version 1.0,2016年11月15日
 * @see
 * @since
 */
public class MergeSchema {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("MergeSchema")
                .setMaster("local");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(javaSparkContext);
        DataFrame idAndNameDF=sqlContext.read().format("json").load("C://Users//yanglin//Desktop//test//megerschema//idandname.txt");
        DataFrame idAndAgeDF=sqlContext.read().format("json").load("C://Users//yanglin//Desktop//test//megerschema//idandage.txt");
        //合并两个元数据
        DataFrame personDF=sqlContext.read().option("mergeSchema", "true").format("json").load("C://Users//yanglin//Desktop//test//megerschema");
        personDF.printSchema();
        personDF.show();
    }

}
