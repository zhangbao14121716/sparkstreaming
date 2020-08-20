package com.atguigu.utils;


import java.util.Random;
/**
 * @Author: Zhangbao
 * @Date: 14:57 2020/8/14
 * @Description:
 */


public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}
