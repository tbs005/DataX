package com.alibaba.datax.plugin.writer.hbasewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;

import static com.alibaba.datax.plugin.writer.hbasewriter.HBaseWriterErrorCode.*;

public class HBaseWriter extends Writer {
	public static final class Job extends Writer.Job {
		private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration originalConfig;
        private String hbaseTable;
        private String hbaseRowkey;
        private String hbaseColumn;
		private Map<String, String> configurationMap;
        private String nullMode;
        private String encoding;
        private int batchRows;

		@SuppressWarnings("unchecked")
		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			
			//在Job中判断配置项格式合法性,以尽早发现配置问题
			hbaseTable = originalConfig.getNecessaryValue(Key.KEY_HBASE_TABLE, HBASE_TABLE_CONF_ERROR);
			hbaseRowkey = originalConfig.getNecessaryValue(Key.KEY_HBASE_ROWKEY, HBASE_ROWKEY_CONF_ERROR);
			hbaseColumn = originalConfig.getNecessaryValue(Key.KEY_HBASE_COLUMN, HBASE_COLUMN_CONF_ERROR);
			
			configurationMap = (Map<String, String>)originalConfig.get(Key.KEY_HBASE_CONF, Map.class);
			if(configurationMap == null || configurationMap.size() <= 0) {
				throw DataXException.asDataXException(HBASE_CONF_ERROR, HBASE_CONF_ERROR.getDescription());
			} else {
				if(!configurationMap.containsKey(Key.KEY_ZK_PARENT)) {
					throw DataXException.asDataXException(ZK_PARENT_CONF_ERROR, ZK_PARENT_CONF_ERROR.getDescription());
				}
				if(!configurationMap.containsKey(Key.KEY_ZK_QUORUM)) {
					throw DataXException.asDataXException(ZK_QUORUM_CONF_ERROR, ZK_QUORUM_CONF_ERROR.getDescription());
				}
				if(!configurationMap.containsKey(Key.KEY_ZK_CLIENTPORT)) {
					throw DataXException.asDataXException(ZK_CLIENTPORT_CONF_ERROR, ZK_CLIENTPORT_CONF_ERROR.getDescription());
				}
			}
			
			nullMode = originalConfig.getUnnecessaryValue(Key.KEY_HBASE_NULLMODE, Const.DEFAULT_NULLMODE, HBASE_NULLMODE_CONF_ERROR);
			encoding = originalConfig.getUnnecessaryValue(Key.KEY_HBASE_ENCODE, Const.DEFAULT_ENCODING, HBASE_ENCODE_CONF_ERROR);
			batchRows = originalConfig.getInt(Key.KEY_HBASE_BATCHROWS, Const.DEFAULT_BATCHROWS);
			
		}

		@Override
		public void destroy() {
			
		}

		@Override
		public List<Configuration> split(int mandatoryNumber) {
			List<Configuration> list = Lists.newArrayList();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
		}
        
	} //public static final class Job extends Writer.Job
	
	
	public static class Task extends Writer.Task {
		private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration sliceConfig;
        private String hbaseTable;
        private String hbaseRowkey;
        private String hbaseColumn;
		private Map<String, String> configurationMap;
        private String nullMode;
        private String encoding;
        private int batchRows;
        private int rowkeyIndex = 0;
        
        private HBaseClient hbaseClient;
        private List<String> hbaseColumnsList = new ArrayList<String>();

		@SuppressWarnings("unchecked")
		@Override
		public void init() {
			this.sliceConfig = super.getPluginJobConf();

			//在Job中判断配置项格式合法性,以尽早发现配置问题
			hbaseTable = sliceConfig.getNecessaryValue(Key.KEY_HBASE_TABLE, HBASE_TABLE_CONF_ERROR);
			hbaseRowkey = sliceConfig.getNecessaryValue(Key.KEY_HBASE_ROWKEY, HBASE_ROWKEY_CONF_ERROR);
			hbaseColumn = sliceConfig.getNecessaryValue(Key.KEY_HBASE_COLUMN, HBASE_COLUMN_CONF_ERROR);
			
			configurationMap = (Map<String, String>)sliceConfig.get(Key.KEY_HBASE_CONF, Map.class);
			if(configurationMap == null || configurationMap.size() <= 0) {
				throw DataXException.asDataXException(HBASE_CONF_ERROR, HBASE_CONF_ERROR.getDescription());
			} else {
				if(!configurationMap.containsKey(Key.KEY_ZK_PARENT)) {
					throw DataXException.asDataXException(ZK_PARENT_CONF_ERROR, ZK_PARENT_CONF_ERROR.getDescription());
				}
				if(!configurationMap.containsKey(Key.KEY_ZK_QUORUM)) {
					throw DataXException.asDataXException(ZK_QUORUM_CONF_ERROR, ZK_QUORUM_CONF_ERROR.getDescription());
				}
				if(!configurationMap.containsKey(Key.KEY_ZK_CLIENTPORT)) {
					throw DataXException.asDataXException(ZK_CLIENTPORT_CONF_ERROR, ZK_CLIENTPORT_CONF_ERROR.getDescription());
				}
			}
			
			nullMode = sliceConfig.getUnnecessaryValue(Key.KEY_HBASE_NULLMODE, Const.DEFAULT_NULLMODE, HBASE_NULLMODE_CONF_ERROR);
			encoding = sliceConfig.getUnnecessaryValue(Key.KEY_HBASE_ENCODE, Const.DEFAULT_ENCODING, HBASE_ENCODE_CONF_ERROR);
			batchRows = sliceConfig.getInt(Key.KEY_HBASE_BATCHROWS, Const.DEFAULT_BATCHROWS);
			if(batchRows <= 0) {
				throw DataXException.asDataXException(HBASE_BATCHROWS_CONF_ERROR, HBASE_BATCHROWS_CONF_ERROR.getDescription());
			}
			if(batchRows > Const.MAX_BATCHROWS) {
				batchRows = Const.MAX_BATCHROWS;
			}
			
			
			String[] elemRowkey = hbaseRowkey.split("\\|");
			if(elemRowkey == null || elemRowkey.length != 2) {
				throw DataXException.asDataXException(HBASE_ROWKEY_CONF_ERROR, HBASE_ROWKEY_CONF_ERROR.getDescription());
			}
			try {
				rowkeyIndex = Integer.valueOf(elemRowkey[0]);
				if(rowkeyIndex < 0) {
					throw DataXException.asDataXException(HBASE_ROWKEY_CONF_ERROR, HBASE_ROWKEY_CONF_ERROR.getDescription());
				}
			} catch(NumberFormatException e) {
				throw DataXException.asDataXException(HBASE_ROWKEY_CONF_ERROR, HBASE_ROWKEY_CONF_ERROR.getDescription());
			}
			hbaseColumnsList.add(rowkeyIndex, hbaseRowkey);
			 
			
			String[] hbaseColumns = hbaseColumn.split(",");
			for(String hColumn : hbaseColumns) {
				String[] elem = hColumn.split("\\|");
				if(elem == null || elem.length != 3) {
					throw DataXException.asDataXException(HBASE_COLUMN_CONF_ERROR, HBASE_COLUMN_CONF_ERROR.getDescription());
				}
				int index = -1;
				try {
					index = Integer.valueOf(elem[0]);
					if(index < 0) {
						throw DataXException.asDataXException(HBASE_COLUMN_CONF_ERROR, HBASE_COLUMN_CONF_ERROR.getDescription());
					}
				} catch(NumberFormatException e) {
					throw DataXException.asDataXException(HBASE_COLUMN_CONF_ERROR, HBASE_COLUMN_CONF_ERROR.getDescription());
				}
				
				hbaseColumnsList.add(index, hColumn);
			}
			
			hbaseClient = new HBaseClient();
			try {
				hbaseClient.init(configurationMap);
			} catch (Exception e) {
				log.error("init"+hbaseTable+" failed.", e);
				throw DataXException.asDataXException(HBASE_CONF_ERROR, HBASE_CONF_ERROR.getDescription());
			}
		}

		@Override
		public void destroy() {
			if(hbaseClient != null) {
				try {
					hbaseClient.close();
				} catch (Exception e) {
					log.error("close table "+hbaseTable+" failed.", e);
					throw DataXException.asDataXException(HBASE_RUNNING_ERROR, HBASE_RUNNING_ERROR.getDescription());
				}
			}
		}

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			int ok = 0;
            int count = 0;
            Record record = null;
            
            //除了第一列rowkey主键外必须还有其他字段
            if(hbaseColumnsList != null && hbaseColumnsList.size() > 1) {
            	List<HBaseCell> cells = new ArrayList<HBaseCell>();
            	
            	while((record = lineReceiver.getFromReader()) != null) {
                	if(hbaseColumnsList.size() != record.getColumnNumber()) {
                		throw DataXException.asDataXException(ILLEGAL_VALUES_ERROR, ILLEGAL_VALUES_ERROR.getDescription() +
                				"读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + hbaseColumnsList.size());
                	}
                	
                	String rowKey = record.getColumn(this.rowkeyIndex).asString();
                	if(rowKey != null && !"".equals(rowKey)) {
                		//loop the cells in the row
                		for(int i = 0; i < hbaseColumnsList.size(); i++) {
                    		String colDescr = hbaseColumnsList.get(i);
                    		String[] elem = colDescr.split("\\|");
                    		
                    		//非主键字段
                    		if(elem != null && elem.length == 3) {
                    			String cf = Const.DEFAULT_COLUMN_FAMILY;
                    			String cq = Const.DEFAULT_QUALIFIER;
                    			
                    			String col = elem[2];
                    			if(col.indexOf(":") != -1) {
                    				cf = col.substring(0, col.indexOf(":"));
                    			}
                    			if((col.indexOf(":")+1) < col.length()) {
                    				cq = col.substring(col.indexOf(":")+1);
                    			}
                    			
                    			String value = record.getColumn(i).asString();
                    			HBaseCell cell = new HBaseCell();
                        		cell.setRowKey(rowKey);
                        		cell.setColf(cf);
                        		cell.setCol(cq);
                        		cell.setValue(value);
                        		cells.add(cell);
                    		}
                    	}
                		
                		//add one row
                		count++;
                		ok++;
                	} //if(rowKey != null && !"".equals(rowKey))
                	else {
                		//收集脏数据
                		super.getTaskPluginCollector().collectDirtyRecord(record, "rowkey字段为空");
                	}
                	
                	
                	if(count >= this.batchRows) {
                		try {
							hbaseClient.put(this.hbaseTable, cells);
						} catch (Exception e) {
							log.error("put table "+this.hbaseTable+" failed.", e);
							cells.clear();
							break;
						}
                		cells.clear();
                		count = 0;
                	}
                } //while((record = lineReceiver.getFromReader()) != null)
            	
            	if(cells != null && cells.size() > 0) {
            		try {
						hbaseClient.put(this.hbaseTable, cells);
					} catch (Exception e) {
						log.error("put table "+this.hbaseTable+" failed.", e);
					}
            	}
            	
            	log.info(ok + " rows are successfully inserted into the table " + this.hbaseTable);
        	} //if(hbaseColumnsList != null && hbaseColumnsList.size() > 1)
		} //public void startWrite(RecordReceiver lineReceiver)
        
	} //public static class Task extends Writer.Task
}
