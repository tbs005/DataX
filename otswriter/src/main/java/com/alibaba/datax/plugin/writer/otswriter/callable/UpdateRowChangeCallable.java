package com.alibaba.datax.plugin.writer.otswriter.callable;

import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowResult;

public class UpdateRowChangeCallable implements Callable<UpdateRowResult>{
    
    private OTS ots = null;
    private UpdateRowRequest updateRowRequest  = null;

    public UpdateRowChangeCallable(OTS ots, UpdateRowRequest updateRowRequest ) {
        this.ots = ots;
        this.updateRowRequest = updateRowRequest;
    }
    
    @Override
    public UpdateRowResult call() throws Exception {
        return ots.updateRow(updateRowRequest);
    }

}
