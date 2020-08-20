package com.atguigu.utils;

/**
 * @Author: Zhangbao
 * @Date: 14:50 2020/8/14
 * @Description:
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
