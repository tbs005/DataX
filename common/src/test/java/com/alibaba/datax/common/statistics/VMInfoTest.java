package com.alibaba.datax.common.statistics;

import org.junit.Assert;
import org.junit.Test;

import java.lang.management.*;
import java.util.Arrays;
import java.util.List;

/**
 * Created by liqiang on 15/11/12.
 */
public class VMInfoTest {
    static final long MB = 1024 * 1024;

    @Test
    public void testOs() throws Exception {

        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        System.out.println(runtimeMXBean.getName());
        System.out.println("jvm运营商:" + runtimeMXBean.getVmVendor());
        System.out.println("jvm规范版本:" + runtimeMXBean.getSpecVersion());
        System.out.println("jvm实现版本:" + runtimeMXBean.getVmVersion());


        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        System.out.println(osMXBean.getName());
        System.out.println(osMXBean.getArch());
        System.out.println(osMXBean.getVersion());
        System.out.println(osMXBean.getAvailableProcessors());


        if (VMInfo.isSunOsMBean(osMXBean)) {
            long totalPhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getTotalPhysicalMemorySize");
            long freePhysicalMemory = VMInfo.getLongFromOperatingSystem(osMXBean, "getFreePhysicalMemorySize");
            System.out.println("总物理内存(M):" + totalPhysicalMemory / MB);
            System.out.println("剩余物理内存(M):" + freePhysicalMemory / MB);

            long maxFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getMaxFileDescriptorCount");
            long currentOpenFileDescriptorCount = VMInfo.getLongFromOperatingSystem(osMXBean, "getOpenFileDescriptorCount");
            long getProcessCpuTime = VMInfo.getLongFromOperatingSystem(osMXBean, "getProcessCpuTime");
            System.out.println(osMXBean.getSystemLoadAverage());
            System.out.println("maxFileDescriptorCount=>" + maxFileDescriptorCount);
            System.out.println("currentOpenFileDescriptorCount=>" + currentOpenFileDescriptorCount);
            System.out.println("jvm运行时间（毫秒）:" + runtimeMXBean.getUptime());
            System.out.println("getProcessCpuTime=>" + getProcessCpuTime);

            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() > startTime + 1000) {
                    break;
                }
            }
//            system = ManagementFactory.getOperatingSystemMXBean();
//            runtime = ManagementFactory.getRuntimeMXBean();
            System.out.println("test!!" + 2 * 2 * 2 * 123456789);
            System.out.println("test!!" + 123456789 * 987654321);
            System.out.println("test!!" + 2 * 2 * 2 * 2);
            System.out.println("test!!" + 3 * 2 * 4);
            System.out.println("test123!!");
            long upTime = runtimeMXBean.getUptime();
            long processTime = VMInfo.getLongFromOperatingSystem(osMXBean, "getProcessCpuTime");
            System.out.println("jvm运行时间（毫秒）:" + upTime);
            System.out.println("getProcessCpuTime=>" + processTime);

            System.out.println(String.format("%,.1f", (float) processTime / (upTime * osMXBean.getAvailableProcessors() * 10000)));


            List<GarbageCollectorMXBean> garbages = ManagementFactory.getGarbageCollectorMXBeans();
            for (GarbageCollectorMXBean garbage : garbages) {
                System.out.println("垃圾收集器：名称=" + garbage.getName() + ",收集=" + garbage.getCollectionCount() + ",总花费时间=" + garbage.getCollectionTime() + ",内存区名称=" + Arrays.deepToString(garbage.getMemoryPoolNames()));
            }

            List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
            if (pools != null && !pools.isEmpty()) {
                for (MemoryPoolMXBean pool : pools) {
                    //只打印一些各个内存区都有的属性，一些区的特殊属性，可看文档或百度
                    // 最大值，初始值，如果没有定义的话，返回-1，所以真正使用时，要注意
                    System.out.println("vm内存区:\n\t名称=" + pool.getName() + "\n\t所属内存管理者=" + Arrays.deepToString(pool.getMemoryManagerNames()) + "\n\t ObjectName=" + "\n\t初始大小(M)=" + pool.getUsage().getInit() / MB + "\n\t最大(上限)(M)=" + pool.getUsage().getMax() / MB + "\n\t已用大小(M)=" + pool.getUsage().getUsed() / MB + "\n\t已提交(已申请)(M)=" + pool.getUsage().getCommitted() / MB + "\n\t使用率=" + (pool.getUsage().getUsed() * 100 / pool.getUsage().getCommitted()) + "%");
                }
            }

        }
    }

    @Test
    public void testVMInfo() throws Exception {
        VMInfo vmInfo = VMInfo.getVmInfo();
        Assert.assertTrue(vmInfo != null);
        System.out.println(vmInfo.toString());
        vmInfo.getDelta();
        int count = 0;

        while(count < 10) {
            long startTime = System.currentTimeMillis();
            while (true) {
                if (System.currentTimeMillis() > startTime + 1000) {
                    break;
                }
            }
            vmInfo.getDelta();
            count++;
            Thread.sleep(1000);
        }

        vmInfo.getDelta(false);
        System.out.println(vmInfo.totalString());
    }
}