package com.alibaba.datax.plugin.writer.hbasewriter;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableCreate {
	private static final Logger log = LoggerFactory.getLogger(HBaseTableCreate.class);
	
	public static Configuration configuration;
	public static Connection connection;
	public static Admin admin;
	
	
	public static void init(String zips, String zport) {
		configuration = HBaseConfiguration.create();
		//configuration.set("hbase.zookeeper.quorum","10.10.3.181,10.10.3.182,10.10.3.183");
		//configuration.set("hbase.zookeeper.property.clientPort","2181");
		configuration.set("hbase.zookeeper.quorum",zips);
		configuration.set("hbase.zookeeper.property.clientPort",zport);
		configuration.set("zookeeper.znode.parent","/hbase");
		
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
			log.error("hbase init exception occured.", e);
		}
	}
	
	public static void close() {
		try {
			if(admin != null) {
				admin.close();
			}
			if(connection != null) {
				connection.close();
			}
		} catch (IOException e) {
			log.error("hbase close exception occured.", e);
		}
	}
	
	public static void createTable(
			String zips, 
			String zport, 
			String tableName, 
			String columnFamily, 
			String startKey, 
			String endKey, 
			String regionCount, 
			String rowKeyType, 
			String comprAlgorithm) throws IOException {
		
		init(zips, zport);
		TableName tableNmae = TableName.valueOf(tableName);
		if(admin.tableExists(tableNmae)) {
			log.error("The table " + tableName + " already exists.");
		} else {
			HTableDescriptor tableDescp = new HTableDescriptor(tableNmae);
			
			HColumnDescriptor colFamilyDescp = new HColumnDescriptor(columnFamily);
			if("snappy".equalsIgnoreCase(comprAlgorithm)) {
				colFamilyDescp.setCompressionType(Algorithm.SNAPPY);
			} else if("gz".equalsIgnoreCase(comprAlgorithm)) {
				colFamilyDescp.setCompressionType(Algorithm.GZ);
			} else if("lz4".equalsIgnoreCase(comprAlgorithm)) {
				colFamilyDescp.setCompressionType(Algorithm.LZ4);
			} else if("lzo".equalsIgnoreCase(comprAlgorithm)) {
				colFamilyDescp.setCompressionType(Algorithm.LZO);
			} else {
				colFamilyDescp.setCompressionType(Algorithm.NONE);
			}
			tableDescp.addFamily(colFamilyDescp);
			
			byte[][] splitKeys = null;
			int regions = Integer.parseInt(regionCount);
			if(rowKeyType != null && "uuid".equalsIgnoreCase(rowKeyType)) {
				splitKeys = getOdpsUuidSplits(startKey, endKey, regions);
			} else {
				splitKeys = getHexSplits(startKey, endKey, regions);
			}
			
			if(splitKeys != null && splitKeys.length > 0) {
				admin.createTable(tableDescp, splitKeys);
			} else {
				log.warn("hbase table created with no pre-splits.");
				admin.createTable(tableDescp);
			}
		}
		close();
	}
	
	public static void main(String[] args) throws Exception {

		Options opts = new Options();
		//-h help
		opts.addOption("h", false, "Print help for this application");
		//-z zookeeper host ips
		opts.addOption("z", true, "The zookeeper host ips, ip1,ip2,ip3");
		//-p zookeeper client port
		opts.addOption("p", true, "The zookeeper client port");
		//-t hbase table name
		opts.addOption("t", true, "The hbase table name");
		//-f column family
		opts.addOption("f", true, "The hbase table column family name");
		//-s start row key
		opts.addOption("s", true, "The start row key");
		//-e end row key
		opts.addOption("e", true, "The end row key");
		//-c region count
		opts.addOption("c", true, "The hbase table region count");
		//-k row key type
		// hex: 16x, format: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
		// uuid: 16x, format: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF
		opts.addOption("k", true, "The row key type, hex: 16x, forsmat: FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF; uuid: 16x, format: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF;");
		//-a compressin algorithm none snappy lzo etc
		opts.addOption("a", true, "The compressin algorithm");
		
		
		BasicParser parser = new BasicParser();
		CommandLine cl = parser.parse(opts, args);
		if(cl.hasOption('h')) {
			HelpFormatter hf = new HelpFormatter();
			hf.printHelp("OptionsTip", opts);
		}
		
		String zips = cl.getOptionValue("z");
		String zport = cl.getOptionValue("p");
		String tableName = cl.getOptionValue("t");
		String columnFamily = cl.getOptionValue("f");
		String startKey = cl.getOptionValue("s");
		String endKey = cl.getOptionValue("e");
		String regionCount = cl.getOptionValue("c");
		String rowKeyType = cl.getOptionValue("k");
		String comprAlgorithm = cl.getOptionValue("a");
		
		if(rowKeyType == null || "".equals(rowKeyType)) {
			log.error("invalid rowKeyType");
			return;
		}
		
		createTable(zips, zport, tableName, columnFamily, startKey, endKey, regionCount, rowKeyType, comprAlgorithm);
		
	}
	
	//startKey 16进制数字字符串
	//endKey 16进制数字字符串
	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions-1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		
		lowestKey = lowestKey.add(regionIncrement);
		for(int i = 0; i < numRegions-1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}	
	
	//startKey 16进制数字字符串 格式,36bits: 00000000-0000-0000-0000-000000000000
	//endKey 16进制数字字符串 格式,36bits: FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF
	public static byte[][] getOdpsUuidSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions-1][];
		
		String[] startKeys = startKey.split("-");
		String[] endKeys = endKey.split("-");
		
		if(startKeys != null && endKeys != null && 
			startKeys.length == endKeys.length) {
			for(int i = 0; i < startKeys.length; i++) {
				if(startKeys[i].length() != endKeys[i].length()) {
					log.error("invalid startKey or endKey, corresponding length does not match.");
					return null;
				}
			}
			

			BigInteger lowestKey = new BigInteger(startKey.replaceAll("-", ""), 16);
			BigInteger highestKey = new BigInteger(endKey.replaceAll("-", ""), 16);
			
			BigInteger range = highestKey.subtract(lowestKey);
			BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
			
			StringBuffer sbuffer = new StringBuffer();
			lowestKey = lowestKey.add(regionIncrement);
			for(int i = 0; i < numRegions-1; i++) {
				BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
				String skey = String.format("%016x", key);
				
				int delta = 0;
				for(int j = 0; j < startKeys.length; j++) {
					int length = startKeys[j].length();
					
					if(j < (startKeys.length-1)) {
						String k = skey.substring(delta, delta+length) + "-";
						sbuffer.append(k);
					} else if(j == (startKeys.length-1)) {
						String k = skey.substring(delta);
						sbuffer.append(k);
					}
					
					delta += length;
				}
				
				
				byte[] b = sbuffer.toString().getBytes();
				splits[i] = b;
				sbuffer.setLength(0);
			}
			
			return splits;
		} else {
			log.error("invalid startKey or endKey, length does not match.");
		}
		
		return null;
	}

}
