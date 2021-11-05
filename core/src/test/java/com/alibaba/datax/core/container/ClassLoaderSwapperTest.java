package com.alibaba.datax.core.container;

import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by jingxing on 14-9-4.
 */
public class ClassLoaderSwapperTest {
    @Test
    public void test() {
        ClassLoaderSwapper classLoaderSwapper =
                ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
        ClassLoader newClassLoader = new URLClassLoader(new URL[]{});
        classLoaderSwapper.setCurrentThreadClassLoader(newClassLoader);
        Assert.assertTrue("", newClassLoader ==
                classLoaderSwapper.restoreCurrentThreadClassLoader());
    }
}
