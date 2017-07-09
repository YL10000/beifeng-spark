/*
 * 项目名：beifeng-spark
 * 文件名：Rdd2DataFrameDynamic.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：动态将rdd转换为DataFrame
 * 修改人：yanglin
 * 修改时间：2016年11月15日 上午10:20:50
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Rdd2DataFrameDynamic
 *	
 * @Description 动态将rdd转换为DataFrame
 * @author yanglin
 * @version 1.0,2016年11月15日
 * @see
 * @since
 */
public class Rdd2DataFrameDynamic {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("Rdd2DataFrameDynamic")
                .setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        
        //使用程序构建DataFrame的元数据
        StructType structType=new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
                
        });
        
        //创建sqlContext
        SQLContext sqlContext=new SQLContext(context);
        
        //创建studentsRdd
        JavaRDD<Row> studentsRdd=context.textFile("src/main/resources/students.txt").map(new Function<String, Row>() {

            private static final long serialVersionUID = 1L;

            public Row call(String line) throws Exception {
                String[] words=line.split(" ");
                return RowFactory.create(Integer.parseInt(words[0]),words[1],Integer.parseInt(words[2]));
            }
        });
        
        //使用动态构建的元数据创建DataFrame
        DataFrame studentDataFrame= sqlContext.createDataFrame(studentsRdd, structType);
        
        sqlContext.registerDataFrameAsTable(studentDataFrame, "students");
        
        /**
         * +---+--------+---+
         | id|    name|age|
         +---+--------+---+
         |  1|zhangsan| 23|
         +---+--------+---+
         */
        studentDataFrame.filter(studentDataFrame.col("id").$eq$eq$eq(1))
             .toJavaRDD()
             .foreach(new VoidFunction<Row>() {
                
                private static final long serialVersionUID = 1L;

                public void call(Row row) throws Exception {
                    System.out.println("{id:"+row.getInt(0)+",name:"+row.getString(1)+",age:"+row.getInt(2)+"}");
                }
            });
        
        
    }

}
