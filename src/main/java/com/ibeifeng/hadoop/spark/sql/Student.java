/*
 * 项目名：beifeng-spark
 * 文件名：Student.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：
 * 修改人：yanglin
 * 修改时间：2016年11月14日 下午8:56:21
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.sql;

import java.io.Serializable;

/**
 * Student
 *	
 * @Description 功能详细描述 
 * @author yanglin
 * @version 1.0,2016年11月14日
 * @see
 * @since
 */
public class Student implements Serializable{

    private static final long serialVersionUID = 1L;

    private Integer id;
    
    private String name;
    
    private Integer age;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Student(Integer id, String name, Integer age) {
        super();
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public Student() {
        super();
    }
    
    
}
