package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.transformer.Transformer;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class FakeReplaceTransformer extends Transformer {
    public FakeReplaceTransformer() {
        setTransformerName("dx_fakeReplace");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        return null;
    }
}
