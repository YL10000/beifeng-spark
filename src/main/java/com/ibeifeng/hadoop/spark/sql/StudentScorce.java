/*
 * 项目名：beifeng-spark
 * 文件名：StudentScorce.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：根据学生成绩查找学生信息
 * 修改人：yanglin
 * 修改时间：2016年11月16日 下午5:43:24
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.ipc.specific.Social;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

/**
 * StudentScorce
 *	
 * @Description 根据学生成绩查找学生信息
 * @author yanglin
 * @version 1.0,2016年11月16日
 * @see
 * @since
 */
public class StudentScorce {

    public static void main(String[] args) {
        SparkConf conf=new SparkConf()
                .setAppName("StudentScorce")
                .setMaster("local");
        JavaSparkContext context=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(context);
        //获取学生分数dataframe
        DataFrame scoresDF = sqlContext.read().json("src/main/resources/scores.txt");
        sqlContext.registerDataFrameAsTable(scoresDF, "scores");
        //获取学生信息dataframe
        DataFrame studentInfosDF=sqlContext.read().json(context.parallelize(Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":17}",
                "{\"name\":\"lisi\",\"age\":25}",
                "{\"name\":\"wangwu\",\"age\":36}"
                )));
        sqlContext.registerDataFrameAsTable(studentInfosDF, "infos");
        
        
        
        /*
        //方法一
        //查询80分以上的学生姓名
        DataFrame goodStudentScoresDF=sqlContext.sql("select name,score from scores where score > 80");
        JavaPairRDD<String, Long> studentScoresRdd=goodStudentScoresDF.toJavaRDD().mapToPair(new PairFunction<Row, String, Long>() {

            private static final long serialVersionUID = 1L;

            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
            }
        });
        
        List<Tuple2<String, Long>> scoresList=studentScoresRdd.collect();
        
        
        StringBuffer sql=new StringBuffer("select name ,age from infos where name in (");
        for(int i=0;i<scoresList.size();i++){
            
            sql.append("\"").append(scoresList.get(i)._1).append("\"");
            if (i==scoresList.size()-1) {
                sql.append(")");
            }else{
                sql.append(",");
            }
        }
        
        //根据上面查出来的学生信息查询信息信息
        JavaPairRDD<String, Long> studentInfosRdd=sqlContext.sql(sql.toString()).javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
            }
        });
        
        //将学生的分数和姓名join在一起
        sqlContext.createDataFrame(studentScoresRdd.join(studentInfosRdd).map(new Function<Tuple2<String,Tuple2<Long,Long>>, Row>() {

                public Row call(Tuple2<String, Tuple2<Long, Long>> tuple)
                        throws Exception {
                    return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
                }
            }), DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("score", DataTypes.LongType, true),
                DataTypes.createStructField("age", DataTypes.LongType, true)
            ))).write().format("json").save("src/main/resources/studentScores");;*/
        
        
       //方法二： 
        sqlContext.sql("select s.name,s.score,i.age from scores s left join infos i on s.name=i.name where s.score > 80")
        .write().format("json").save("src/main/resources/studentScoresjoin");
        
    }

}
