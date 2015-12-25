package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.scaffold.ColumnProducer;
import com.alibaba.datax.core.scaffold.ConfigurationProducer;
import com.alibaba.datax.core.scaffold.RecordProducer;
import com.alibaba.datax.core.scaffold.base.CaseInitializer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.channel.memory.MemoryChannel;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;

public class RecordExchangerTest extends CaseInitializer {

	private Configuration configuration = null;

	@Before
	public void before() {
		this.configuration = ConfigurationProducer.produce();
		this.configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);
		return;
	}


	@Test
	public void testMemeroySize() throws Exception {
		Column longColumn = ColumnProducer.produceLongColumn(1);
		Column longColumn2 = new LongColumn("234567891");
		Column stringColumn= ColumnProducer.produceStringColumn("sringtest");
		Column boolColumn=ColumnProducer.produceBoolColumn(true);
		Column dateColumn = ColumnProducer.produceDateColumn(System.currentTimeMillis());
		Column bytesColumn = ColumnProducer.produceBytesColumn("test".getBytes("utf-8"));
		Assert.assertEquals(longColumn.getByteSize(),8);
		Assert.assertEquals(longColumn2.getByteSize(),9);
		Assert.assertEquals(stringColumn.getByteSize(),9);
		Assert.assertEquals(boolColumn.getByteSize(),1);
		Assert.assertEquals(dateColumn.getByteSize(),8);
		Assert.assertEquals(bytesColumn.getByteSize(),4);

		Record record = new DefaultRecord();
		record.addColumn(longColumn);
		record.addColumn(longColumn2);
		record.addColumn(stringColumn);
		record.addColumn(boolColumn);
		record.addColumn(dateColumn);
		record.addColumn(bytesColumn);

		Assert.assertEquals(record.getByteSize(),39);
		// record classSize =  80
		// column classSize = 6*24
		Assert.assertEquals(record.getMemorySize(),263);

	}

	@Test
	public void test_Exchanger() {
		Channel channel = new MemoryChannel(configuration);
        channel.setCommunication(new Communication());

		int capacity = 10;
		Record record = null;
		RecordExchanger recordExchanger = new RecordExchanger(channel);

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			recordExchanger.sendToWriter(record);
		}

		System.out.println("byteSize=" + record.getByteSize());
		System.out.println("meorySize=" + record.getMemorySize());

		channel.close();

		int counter = 0;
		while ((record = recordExchanger.getFromReader()) != null) {
			System.out.println(record.getColumn(0).toString());
			Assert.assertTrue(record.getColumn(0).asLong() == counter);
			counter++;
		}

		Assert.assertTrue(capacity == counter);
	}

	@Test
	public void test_BufferExchanger() {

		Configuration configuration = ConfigurationProducer.produce();
		configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);

		Channel channel = new MemoryChannel(configuration);
        channel.setCommunication(new Communication());

		TaskPluginCollector pluginCollector = mock(TaskPluginCollector.class);
		int capacity = 10;
		Record record = null;
		BufferedRecordExchanger recordExchanger = new BufferedRecordExchanger(
				channel,pluginCollector);

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			recordExchanger.sendToWriter(record);
		}

		recordExchanger.flush();

		channel.close();

		int counter = 0;
		while ((record = recordExchanger.getFromReader()) != null) {
			System.out.println(record.getColumn(0).toString());
			Assert.assertTrue(record.getColumn(0).asLong() == counter);
			counter++;
		}

		System.out.println(String.format("Capacity: %d Counter: %d .",
				capacity, counter));
		Assert.assertTrue(capacity == counter);
	}

	@Test
	public void test_BufferExchanger_单条超过buffer的脏数据() throws Exception {

		Configuration configuration = ConfigurationProducer.produce();
		configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);

		//测试单挑记录超过buffer大小
		configuration.set(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 3);

		TaskPluginCollector pluginCollector = mock(TaskPluginCollector.class);
		int capacity = 10;
		Record record = null;

		Channel channel2 = new MemoryChannel(configuration);
		channel2.setCommunication(new Communication());
		BufferedRecordExchanger recordExchanger2 = new BufferedRecordExchanger(
				channel2,pluginCollector);

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			recordExchanger2.sendToWriter(record);
		}

		ArgumentCaptor<Record> rgArg = ArgumentCaptor.forClass(Record.class);
		ArgumentCaptor<Exception> eArg = ArgumentCaptor.forClass(Exception.class);

		verify(pluginCollector,times(10)).collectDirtyRecord(rgArg.capture(), eArg.capture());

		recordExchanger2.flush();

		channel2.close();

		int counter = 0;
		while ((record = recordExchanger2.getFromReader()) != null) {
			System.out.println(record.getColumn(0).toString());
			Assert.assertTrue(record.getColumn(0).asLong() == counter);
			counter++;
		}

		System.out.println(String.format("Capacity: %d Counter: %d .",
				capacity, counter));
		Assert.assertTrue(counter == 0);

	}

	@Test
	public void test_BufferExchanger_不满32条到达buffer大小() throws Exception {

		Configuration configuration = ConfigurationProducer.produce();
		configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);
		configuration.set(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 500);

		TaskPluginCollector pluginCollector = mock(TaskPluginCollector.class);
		final int capacity = 10;
		Record record = null;

		//测试单挑记录超过buffer大小

		Channel channel3 = new MemoryChannel(configuration);
		channel3.setCommunication(new Communication());
		final BufferedRecordExchanger recordExchangerWriter = new BufferedRecordExchanger(
				channel3,pluginCollector);

		final BufferedRecordExchanger recordExchangerReader = new BufferedRecordExchanger(
				channel3,pluginCollector);

		final BufferedRecordExchanger spy1=spy(recordExchangerWriter);

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				int counter = 0;
				Record record;
				while ((record = recordExchangerReader.getFromReader()) != null) {
					System.out.println(record.getColumn(0).toString());
					Assert.assertTrue(record.getColumn(0).asLong() == counter);
					counter++;
				}

				System.out.println(String.format("Capacity: %d Counter: %d .",
						capacity, counter));
				Assert.assertTrue(capacity == counter);
			}
		});
		t.start();

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			spy1.sendToWriter(record);
		}

		spy1.flush();

		channel3.close();

		t.join();

		verify(spy1,times(5)).flush();

	}

	@Test
	public void test_BufferExchanger_每条大小刚好是buffersize() throws Exception {

		Configuration configuration = ConfigurationProducer.produce();
		configuration.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 1);
		configuration.set(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 229);

		TaskPluginCollector pluginCollector = mock(TaskPluginCollector.class);
		final int capacity = 10;
		Record record = null;

		//测试单挑记录超过buffer大小

		Channel channel3 = new MemoryChannel(configuration);
		channel3.setCommunication(new Communication());
		final BufferedRecordExchanger recordExchangerWriter = new BufferedRecordExchanger(
				channel3,pluginCollector);

		final BufferedRecordExchanger recordExchangerReader = new BufferedRecordExchanger(
				channel3,pluginCollector);

		final BufferedRecordExchanger spy1=spy(recordExchangerWriter);

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				int counter = 0;
				Record record;
				while ((record = recordExchangerReader.getFromReader()) != null) {
					System.out.println(record.getColumn(0).toString());
					Assert.assertTrue(record.getColumn(0).asLong() == counter);
					counter++;
				}

				System.out.println(String.format("Capacity: %d Counter: %d .",
						capacity, counter));
				Assert.assertTrue(capacity == counter);
			}
		});
		t.start();

		for (int i = 0; i < capacity; i++) {
			record = RecordProducer.produceRecord();
			record.setColumn(0, new LongColumn(i));
			spy1.sendToWriter(record);
		}

		spy1.flush();

		channel3.close();

		t.join();

		verify(spy1,times(10)).flush();

	}
}
