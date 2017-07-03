/*
 * 项目名：beifeng-spark
 * 文件名：StudentScoreJDBC.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：根据学生成绩查找学生信息jdbc版本
 * 修改人：yanglin
 * 修改时间：2016年11月17日 下午3:23:58
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

/**
 * StudentScoreJDBC
 *	
 * @Description 根据学生成绩查找学生信息jdbc版本
 * @author yanglin
 * @version 1.0,2016年11月17日
 * @see
 * @since
 */
public class StudentScoreJDBC {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("StudentScoreJDBC").setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        
        //配置mysql数据库的连接
        Map<String, String> options=new HashMap<String, String>();
        options.put("url", "jdbc:mysql://hadoop-senior.ibeifeng.com:3306/spark");
        options.put("user", "root");
        options.put("password", "123456");
        
        //读取student_score表中数据加载到scoreRdd
        options.put("dbtable", "student_score");
        JavaPairRDD<String, Integer> scoreRdd=sqlContext.read().format("jdbc").options(options).load()
            .toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<String, Integer> call(Row row) throws Exception {
                    return new Tuple2<String, Integer>(row.getString(0),row.getInt(1));
                }
            });
        
        //读取student_info表中数据加载到infoRdd
        options.put("dbtable", "student_info");
        JavaPairRDD<String, Integer> infoRdd=sqlContext.read().format("jdbc").options(options).load()
            .toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                private static final long serialVersionUID = 1L;
                public Tuple2<String, Integer> call(Row row) throws Exception {
                    return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
                }
            });
        
        //将scoreRdd join infoRdd ，然后过滤出分数大于80的
        JavaRDD<Row> goodStudentRdd=scoreRdd.join(infoRdd).map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

            private static final long serialVersionUID = 1L;

            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
                    throws Exception {
                return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
            }
        }).filter(new Function<Row, Boolean>() {

            private static final long serialVersionUID = 1L;

            public Boolean call(Row row) throws Exception {
                return row.getInt(1)>80;
            }
        });
        
        Properties connectionProperties=new Properties();
        connectionProperties.setProperty("user", "root");
        connectionProperties.setProperty("password", "123456");
        sqlContext.createDataFrame(goodStudentRdd, DataTypes.createStructType(
                Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("score", DataTypes.IntegerType, true),
                        DataTypes.createStructField("age", DataTypes.IntegerType, true)
                )))
        //将数据保存的mysql中 的good_student_info表中
        .write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop-senior.ibeifeng.com:3306/spark", "good_student_info", connectionProperties);
    }
}
