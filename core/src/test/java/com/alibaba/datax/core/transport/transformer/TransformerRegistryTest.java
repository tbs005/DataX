package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/8.
 */
public class TransformerRegistryTest extends CaseInitializer {
    @Test
    public void testTransformerRegistry() throws Exception {
        TransformerInfo result = TransformerRegistry.getTransformer("test");
        Assert.assertEquals(result, null);

        result = TransformerRegistry.getTransformer("dx_substr");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "dx_substr");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass(), SubstrTransformer.class);
        Assert.assertEquals(result.getClassLoader(),null);
        Assert.assertEquals(result.isNative(),true);

        result = TransformerRegistry.getTransformer("dx_pad");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "dx_pad");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass(), PadTransformer.class);
        Assert.assertEquals(result.getClassLoader(),null);
        Assert.assertEquals(result.isNative(),true);

        result = TransformerRegistry.getTransformer("dx_replace");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "dx_replace");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass(), ReplaceTransformer.class);
        Assert.assertEquals(result.getClassLoader(),null);
        Assert.assertEquals(result.isNative(),true);

        result = TransformerRegistry.getTransformer("dx_filter");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "dx_filter");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass(), FilterTransformer.class);
        Assert.assertEquals(result.getClassLoader(),null);
        Assert.assertEquals(result.isNative(),true);

        result = TransformerRegistry.getTransformer("dx_groovy");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "dx_groovy");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass(), GroovyTransformer.class);
        Assert.assertEquals(result.getClassLoader(),null);
        Assert.assertEquals(result.isNative(),true);


        List<String> lists = new ArrayList<String>();
        lists.add("userTransformerTest");
        lists.add("userComplexTransformerTest");

        TransformerRegistry.loadTransformerFromLocalStorage(lists);

        result = TransformerRegistry.getTransformer("userTransformerTest");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "userTransformerTest");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals(((ComplexTransformerProxy) result.getTransformer()).getRealTransformer().getClass().getName(), "com.alibaba.transformer.gongan.TransformerTest");
        Assert.assertTrue(result.getClassLoader()!=null);
        Assert.assertEquals(result.isNative(),false);

        result = TransformerRegistry.getTransformer("userComplexTransformerTest");
        Assert.assertEquals(result.getTransformer().getTransformerName(), "userComplexTransformerTest");
        Assert.assertEquals(result.getClass(), TransformerInfo.class);
        Assert.assertEquals((result.getTransformer()).getClass().getName(), "com.alibaba.transformer.gongan.ComplexTranformerTest");
        Assert.assertTrue(result.getClassLoader() != null);
        Assert.assertEquals(result.isNative(),false);

    }

}