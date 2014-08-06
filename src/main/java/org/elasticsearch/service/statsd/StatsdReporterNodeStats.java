package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.transport.TransportStats;
import org.elasticsearch.threadpool.ThreadPoolStats;

public class StatsdReporterNodeStats extends StatsdReporter {

    private final NodeStats nodeStats;

    public StatsdReporterNodeStats(NodeStats nodeStats) {
        this.nodeStats = nodeStats;
    }

    public void run() {
        try {
            this.sendNodeFsStats(this.nodeStats.getFs());
            this.sendNodeJvmStats(this.nodeStats.getJvm());
            this.sendNodeNetworkStats(this.nodeStats.getNetwork());
            this.sendNodeOsStats(this.nodeStats.getOs());
            this.sendNodeProcessStats(this.nodeStats.getProcess());
            this.sendNodeHttpStats(this.nodeStats.getHttp());
            this.sendNodeTransportStats(this.nodeStats.getTransport());
            this.sendNodeThreadPoolStats(this.nodeStats.getThreadPool());
        } catch (Exception e) {
            this.logException(e);
        }
    }

    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats)
    {
        String type = this.buildMetricName("node.threadpool");
        Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
        while (statsIterator.hasNext()) {
            ThreadPoolStats.Stats stats = statsIterator.next();
            String id = type + "." + stats.getName();

            this.sendGauge(id, "threads", stats.getThreads());
            this.sendGauge(id, "queue", stats.getQueue());
            this.sendGauge(id, "active", stats.getActive());
            this.sendGauge(id, "rejected", stats.getRejected());
            this.sendGauge(id, "largest", stats.getLargest());
            this.sendGauge(id, "completed", stats.getCompleted());
        }
    }

    private void sendNodeTransportStats(TransportStats transportStats)
    {
        String type = this.buildMetricName("node.transport");
        this.sendGauge(type, "serverOpen", transportStats.serverOpen());
        this.sendCount(type, "rxCount", transportStats.rxCount());
        this.sendCount(type, "rxSizeBytes", transportStats.rxSize().bytes());
        this.sendCount(type, "txCount", transportStats.txCount());
        this.sendCount(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats)
    {
        String type = this.buildMetricName("node.process");

        this.sendGauge(type, "openFileDescriptors", processStats.openFileDescriptors());
        if (processStats.cpu() != null) {
            this.sendGauge(type + ".cpu", "percent", processStats.cpu().percent());
            this.sendGauge(type + ".cpu", "sysSeconds", processStats.cpu().sys().seconds());
            this.sendGauge(type + ".cpu", "totalSeconds", processStats.cpu().total().seconds());
            this.sendGauge(type + ".cpu", "userSeconds", processStats.cpu().user().seconds());
        }

        if (processStats.mem() != null) {
            this.sendGauge(type + ".mem", "totalVirtual", processStats.mem().totalVirtual().bytes());
            this.sendGauge(type + ".mem", "resident", processStats.mem().resident().bytes());
            this.sendGauge(type + ".mem", "share", processStats.mem().share().bytes());
        }
    }

    private void sendNodeOsStats(OsStats osStats)
    {
        String type = this.buildMetricName("node.os");

        if (osStats.cpu() != null) {
            this.sendGauge(type + ".cpu", "sys", osStats.cpu().sys());
            this.sendGauge(type + ".cpu", "idle", osStats.cpu().idle());
            this.sendGauge(type + ".cpu", "user", osStats.cpu().user());
        }

        if (osStats.mem() != null) {
            this.sendGauge(type + ".mem", "freeBytes", osStats.mem().free().bytes());
            this.sendGauge(type + ".mem", "usedBytes", osStats.mem().used().bytes());
            this.sendGauge(type + ".mem", "freePercent", osStats.mem().freePercent());
            this.sendGauge(type + ".mem", "usedPercent", osStats.mem().usedPercent());
            this.sendGauge(type + ".mem", "actualFreeBytes", osStats.mem().actualFree().bytes());
            this.sendGauge(type + ".mem", "actualUsedBytes", osStats.mem().actualUsed().bytes());
        }

        if (osStats.swap() != null) {
            this.sendGauge(type + ".swap", "freeBytes", osStats.swap().free().bytes());
            this.sendGauge(type + ".swap", "usedBytes", osStats.swap().used().bytes());
        }
    }

    private void sendNodeNetworkStats(NetworkStats networkStats)
    {
        String type = this.buildMetricName("node.network.tcp");
        NetworkStats.Tcp tcp = networkStats.tcp();

        // might be null, if sigar isnt loaded
        if (tcp != null) {
            this.sendGauge(type, "activeOpens", tcp.activeOpens());
            this.sendGauge(type, "passiveOpens", tcp.passiveOpens());
            this.sendGauge(type, "attemptFails", tcp.attemptFails());
            this.sendGauge(type, "estabResets", tcp.estabResets());
            this.sendGauge(type, "currEstab", tcp.currEstab());
            this.sendGauge(type, "inSegs", tcp.inSegs());
            this.sendGauge(type, "outSegs", tcp.outSegs());
            this.sendGauge(type, "retransSegs", tcp.retransSegs());
            this.sendGauge(type, "inErrs", tcp.inErrs());
            this.sendGauge(type, "outRsts", tcp.outRsts());
        }
    }

    private void sendNodeJvmStats(JvmStats jvmStats)
    {
        String type = this.buildMetricName("node.jvm");
        this.sendGauge(type, "uptime", jvmStats.uptime().seconds());

        // mem
        this.sendGauge(type + ".mem", "heapCommitted", jvmStats.mem().heapCommitted().bytes());
        this.sendGauge(type + ".mem", "heapUsed", jvmStats.mem().heapUsed().bytes());
        this.sendGauge(type + ".mem", "nonHeapCommitted", jvmStats.mem().nonHeapCommitted().bytes());
        this.sendGauge(type + ".mem", "nonHeapUsed", jvmStats.mem().nonHeapUsed().bytes());

        Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.mem().iterator();
        while (memoryPoolIterator.hasNext()) {
            JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
            String memoryPoolType = type + ".mem.pool." + memoryPool.name();

            this.sendGauge(memoryPoolType, "max", memoryPool.max().bytes());
            this.sendGauge(memoryPoolType, "used", memoryPool.used().bytes());
            this.sendGauge(memoryPoolType, "peakUsed", memoryPool.peakUsed().bytes());
            this.sendGauge(memoryPoolType, "peakMax", memoryPool.peakMax().bytes());
        }

        // threads
        this.sendGauge(type + ".threads", "count", jvmStats.threads().count());
        this.sendGauge(type + ".threads", "peakCount", jvmStats.threads().peakCount());

        // garbage collectors
        long gcCounter = 0;
        long gcTimes = 0;
        for (GarbageCollector gc : jvmStats.gc()) {
            gcCounter += gc.collectionCount();
            gcTimes += gc.collectionTime().getSeconds();
        }
        this.sendCount(type + ".gc", "collectionCount", gcCounter);
        this.sendTime(type + ".gc", "collectionTimeSeconds", gcTimes);
        for (JvmStats.GarbageCollector collector : jvmStats.gc().collectors()) {
            String id = type + ".gc." + collector.name();
            this.sendCount(id, "collectionCount", collector.collectionCount());
            this.sendTime(id, "collectionTimeSeconds", collector.collectionTime().seconds());

            JvmStats.GarbageCollector.LastGc lastGc = collector.lastGc();
            String lastGcType = type + ".lastGc";
            if (lastGc != null) {
                this.sendTime(lastGcType, "time", lastGc.endTime() - lastGc.startTime());
                this.sendGauge(lastGcType, "max", lastGc.max().bytes());
                this.sendGauge(lastGcType, "beforeUsed", lastGc.beforeUsed().bytes());
                this.sendGauge(lastGcType, "afterUsed", lastGc.afterUsed().bytes());
                this.sendGauge(lastGcType, "durationSeconds", lastGc.duration().seconds());
            }
        }
    }

    private void sendNodeHttpStats(HttpStats httpStats)
    {
        String type = this.buildMetricName("node.http");
        this.sendGauge(type, "serverOpen", httpStats.getServerOpen());
        this.sendGauge(type, "totalOpen", httpStats.getTotalOpen());
    }

    private void sendNodeFsStats(FsStats fs)
    {
        Iterator<FsStats.Info> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = this.buildMetricName("node.fs") + i;
            FsStats.Info info = infoIterator.next();
            this.sendGauge(type, "available", info.getAvailable().bytes());
            this.sendGauge(type, "total", info.getTotal().bytes());
            this.sendGauge(type, "free", info.getFree().bytes());
            this.sendCount(type, "diskReads", info.getDiskReads());
            this.sendCount(type, "diskReadsInBytes", info.getDiskReadSizeInBytes());
            this.sendCount(type, "diskWrites", info.getDiskWrites());
            this.sendCount(type, "diskWritesInBytes", info.getDiskWriteSizeInBytes());
            this.sendGauge(type, "diskQueue", (long) info.getDiskQueue());
            this.sendGauge(type, "diskService", (long) info.getDiskServiceTime());
            i++;
        }
    }
}
