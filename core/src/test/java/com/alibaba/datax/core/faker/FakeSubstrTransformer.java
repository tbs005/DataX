package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.transformer.Transformer;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class FakeSubstrTransformer extends Transformer {
    public FakeSubstrTransformer() {
        setTransformerName("dx_fakeSubstr");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        return null;
    }
}
