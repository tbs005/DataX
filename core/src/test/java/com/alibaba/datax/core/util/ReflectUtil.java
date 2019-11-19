package com.alibaba.datax.core.util;

import java.lang.reflect.Field;

/**
 * Created by hongjiao.hj on 2014/12/17.
 */
public class ReflectUtil {

    public static void setField(Object targetObj, String name, Object obj) throws NoSuchFieldException, IllegalAccessException {
        //Class clazz = targetObj.getClass();
        Field field = getDeclaredField(targetObj,name);
        field.setAccessible(true);
        field.set(targetObj, obj);
    }

    public static Object getField(Object targetObj, String name, Object obj) throws NoSuchFieldException, IllegalAccessException {
        Class clazz = targetObj.getClass();
        Field field = clazz.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(targetObj);
    }


    private static Field getDeclaredField(Object object, String fieldName){
        Field field = null ;

        Class<?> clazz = object.getClass() ;

        for(; clazz != Object.class ; clazz = clazz.getSuperclass()) {
            try {
                field = clazz.getDeclaredField(fieldName) ;
                return field ;
            } catch (Exception e) {
                //这里什么都不要做！并且这里的异常必须这样写，不能抛出去。
                //如果这里的异常打印或者往外抛，则就不会执行clazz = clazz.getSuperclass(),最后就不会进入到父类中了

            }
        }

        return null;
    }
}
