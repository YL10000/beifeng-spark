/*
 * 项目名：beifeng-spark
 * 文件名：Rdd2DataFrameReflect.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：使用反射技术将rdd转换为dataframe
 * 修改人：yanglin
 * 修改时间：2016年11月14日 下午8:53:32
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Rdd2DataFrameReflect
 *	
 * @Description 使用反射技术将rdd转换为dataframe
 * @author yanglin
 * @version 1.0,2016年11月14日
 * @see
 * @since
 */
public class Rdd2DataFrameReflect {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("Rdd2DataFrameReflect")
                .setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        
        //创建sqlContext
        SQLContext sqlContext=new SQLContext(context);
        
        //封装为Student JavaRDD
        JavaRDD<Student> students=context.textFile("src/main/resources/students.txt").map(new Function<String, Student>() {

            private static final long serialVersionUID = 1L;

            public Student call(String line) throws Exception {
                String[] words=line.split(" ");
                return new Student(Integer.parseInt(words[0]), words[1], Integer.parseInt(words[2]));
            }
        });
        
        //使用反射技术,将javaRdd转换为DataFrame，使用javabean的属性定义DataFrame的元数据
        DataFrame studentDataFrame= sqlContext.createDataFrame(students, Student.class);
        
        //将dataFrame注册为临时表students
        sqlContext.registerDataFrameAsTable(studentDataFrame, "students");
        
        /**
         * 使用sql语句来操作DataFrame
         */
        JavaRDD<Student> studentsRDD=sqlContext.sql("select id,name,age from students where age >20 ").toJavaRDD().map(new Function<Row, Student>() {

            @Override
            public Student call(Row v1) throws Exception {
                //通过顺序表来获取row中对应的字段
                return new Student(v1.getInt(0),v1.getString(1),v1.getInt(2));
            }
        });
        System.out.println(studentsRDD.collect());
        
        
        
        /**
         * 使用方法操作DataFrame
         */
        //查询指定条件的数据
        JavaRDD<Student> studentsRdd=studentDataFrame.filter(studentDataFrame.col("id").$eq$eq$eq(1))
            
            //将dataFrame数据转换为JavaRDD
            .toJavaRDD()
            .map(new Function<Row, Student>() {

                @Override
                public Student call(Row v1) throws Exception {
                    //通过指定列表来获取row中对应的字段
                    return new Student((Integer)v1.getAs("id"), (String)v1.getAs("name"), (Integer)v1.getAs("age"));
                }
            });
        System.out.println(studentsRdd.collect());
            
            /**
             * +---+---+--------+
               |age| id|    name|
               +---+---+--------+
               | 23|  1|zhangsan|
               +---+---+--------+
             */
            /*.foreach(new VoidFunction<Row>() {
                
                private static final long serialVersionUID = 1L;

                public void call(Row row) throws Exception {
                    System.out.println("{id:"+row.getInt(1)+",name:"+row.getString(2)+",age:"+row.getInt(0)+"}");
                }
            });*/
    }

}
