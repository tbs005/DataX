package com.alibaba.datax.plugin.writer.hbasewriter;

/**
 * Created by qifeng.sxm on 2015/12/11.
 */
public class Const {
	public static final String DEFAULT_NULLMODE = "EMPTY_BYTES";
	public static final String DEFAULT_ENCODING = "utf-8";
	public static final int DEFAULT_BATCHROWS = 100;
	public static final int MAX_BATCHROWS = 1000;
	public static final String DEFAULT_COLUMN_FAMILY = "cf";
	public static final String DEFAULT_QUALIFIER = "unknown";
}
