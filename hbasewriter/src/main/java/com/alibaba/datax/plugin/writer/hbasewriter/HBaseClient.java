package com.alibaba.datax.plugin.writer.hbasewriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseClient {
	private final Logger log = LoggerFactory.getLogger(this.getClass());
	
	private Configuration configuration;
	private Connection connection;
	
	public void init(Map<String, String> confs) throws Exception {
		configuration = HBaseConfiguration.create();
		if(confs != null && confs.size() > 0) {
			for(String key : confs.keySet()) {
				configuration.set(key, confs.get(key));
			}
		}
		
		try {
			this.connection = ConnectionFactory.createConnection(configuration);
		} catch (Exception e) {
			log.error("hbase init exception occured.", e);
			throw e;
		}
	}
	
	public void close() throws Exception {
		try {
			if(connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			log.error("hbase close exception occured.", e);
			throw e;
		}
	}
	
		
	public Table getTable(String tableName) throws Exception {
		Table table = null;
		try {
			if(connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
			}
		} catch (Exception e) {
			log.error("get table "+tableName+" failed.", e);
			throw e;
		}
		return table;
	}

	
	public void put(String tableName, HBaseCell cell) throws Exception {
		
		Table table = getTable(tableName);
		
		Put p = new Put(Bytes.toBytes(cell.getRowKey()));
		p.addColumn(Bytes.toBytes(cell.getColf()), 
					Bytes.toBytes(cell.getCol()),
			        Bytes.toBytes(cell.getValue()));
		
		try {
			table.put(p);
		} catch (Exception e) {
			log.error("put table "+tableName+" failed.", e);
			throw e;
		} finally {
			if(table != null) {
				try {
					table.close();
				} catch (Exception e) {
					log.error("close table "+tableName+" failed.", e);
					throw e;
				}
			}
		}
	}
	
	public void put(String tableName, List<HBaseCell> cells) throws Exception {
		List<Put> puts = new ArrayList<Put>();
		if(cells != null && cells.size() > 0) {
			Table table = getTable(tableName);
			
			for(HBaseCell cell : cells) {
				
				Put p = new Put(Bytes.toBytes(cell.getRowKey()));
				p.addColumn(Bytes.toBytes(cell.getColf()), 
							Bytes.toBytes(cell.getCol()==null?Const.DEFAULT_QUALIFIER:cell.getCol()),
					        Bytes.toBytes(cell.getValue()==null?"":cell.getValue()));
				puts.add(p);
			}
			
			try {
				table.put(puts);
			} catch (Exception e) {
				log.error("put table "+tableName+" failed.", e);
				throw e;
			} finally {
				if(table != null) {
					try {
						table.close();
					} catch (Exception e) {
						log.error("close table "+tableName+" failed.", e);
						throw e;
					}
				}
			}
		} //if(cells != null && cells.size() > 0)
	}
	
}
