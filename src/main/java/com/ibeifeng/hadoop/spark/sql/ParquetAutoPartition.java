/*
 * 项目名：beifeng-spark
 * 文件名：ParquetAutoPartition.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：sqlContext 加载parquet文件自动推断分区列
 * 修改人：yanglin
 * 修改时间：2017年7月9日 下午2:46:20
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * ParquetAutoPartition
 *	
 * @Description sqlContext 加载parquet文件自动推断分区列
 * @author yanglin
 * @version 1.0,2017年7月9日
 * @see
 * @since
 */
public class ParquetAutoPartition {
    
    public static void main(String[] args) {
         SparkConf sparkConf=new SparkConf().setAppName(ParquetAutoPartition.class.getName()).setMaster("local");
         JavaSparkContext sparkContext=new JavaSparkContext(sparkConf);
         SQLContext sqlContext=new SQLContext(sparkContext);
         
         //加载parquet文件为DataFrame
         DataFrame usersDF=sqlContext.read().parquet("src/main/resources/parquet/users.parquet");
         
         /**
          * root
             |-- name: string (nullable = true)
             |-- age: long (nullable = true)
          */
         usersDF.printSchema();
         
         /**
          * +---------+---+
          * |     name|age|
          * +---------+---+
          * |zhangshan| 23|
          * |     lisi| 18|
          * |   wangwu| 36|
          * +---------+---+
          */
         usersDF.show();
         
         /**
          * 加载区别表中的数据是会自动推断分区列
          */
         usersDF=sqlContext.read().parquet("src/main/resources/parquet/female=male/coutry=US/users.parquet");
         
         /**
          * root
             |-- name: string (nullable = true)
             |-- age: long (nullable = true)
             |-- female: string (nullable = true)
             |-- coutry: string (nullable = true)
          */
         usersDF.printSchema();
         
         /**
            +---------+---+------+------+
            |     name|age|female|coutry|
            +---------+---+------+------+
            |zhangshan| 23|  male|    US|
            |     lisi| 18|  male|    US|
            |   wangwu| 36|  male|    US|
            +---------+---+------+------+
          */
         usersDF.show();
         sparkContext.stop();
    }
}
