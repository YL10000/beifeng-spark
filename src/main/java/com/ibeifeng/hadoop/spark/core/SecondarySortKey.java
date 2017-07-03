/*
 * 项目名：beifeng-spark
 * 文件名：SecondarySortKey.java
 * 版权：Copyright (c) 2014-2015 Beijing BiYouWoShi Tech. Co. Ltd. All Rights Reserved.
 * 描述：自定义二次排序的key
 * 修改人：yanglin
 * 修改时间：2016年11月7日 下午6:25:31
 * 修改内容：
 * 版本编号：1.0
 */
package com.ibeifeng.hadoop.spark.core;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * SecondarySortKey
 *	
 * @Description 自定义二次排序的key
 * @author yanglin
 * @version 1.0,2016年11月7日
 * @see
 * @since
 */
public class SecondarySortKey implements Ordered<SecondarySortKey> ,Serializable {
    
    private static final long serialVersionUID = 1L;

    //定义要排序的列
    private int first;
    
    private int second;
    
    public SecondarySortKey(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }

    public boolean $greater(SecondarySortKey other) {
        if (this.first>other.first) {
            return true;
        }else if (this.second>other.second) {
            return true;
        }
        return false;
    }

    public boolean $greater$eq(SecondarySortKey other) {
        if (this.first>=other.first) {
            return true;
        }else if (this.second>=other.second) {
            return true;
        }
        return false;
    }

    public boolean $less(SecondarySortKey other) {
        if (this.first<other.first) {
            return true;
        }else if (this.second<other.second) {
            return true;
        }
        return false;
    }

    public boolean $less$eq(SecondarySortKey other) {
        if (this.first<=other.first) {
            return true;
        }else if (this.second<=other.second) {
            return true;
        }
        return false;
    }

    public int compare(SecondarySortKey other) {
        if (this.first-other.first!=0) {
            return this.first-other.first;
        }else if (this.second-other.second!=0) {
            return this.second-other.second;
        }
        return 0;
    }

    public int compareTo(SecondarySortKey other) {
        if (this.first-other.first!=0) {
            return this.first-other.first;
        }else if (this.second-other.second!=0) {
            return this.second-other.second;
        }
        return 0;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + first;
        result = prime * result + second;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SecondarySortKey other = (SecondarySortKey) obj;
        if (first != other.first)
            return false;
        if (second != other.second)
            return false;
        return true;
    }

    
    
}
