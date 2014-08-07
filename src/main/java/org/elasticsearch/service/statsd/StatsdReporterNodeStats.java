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
	private final String nodeName;
	private final Boolean statsdReportFsDetails;

	public StatsdReporterNodeStats(NodeStats nodeStats, String nodeName, Boolean statsdReportFsDetails) {
		this.nodeStats = nodeStats;
		this.nodeName = nodeName;
		this.statsdReportFsDetails = statsdReportFsDetails;
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

	private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
		String type = this.getPrefix("thread_pool");
		Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
		while (statsIterator.hasNext()) {
			ThreadPoolStats.Stats stats = statsIterator.next();
			String id = type + "." + stats.getName();

			this.sendGauge(id, "threads", stats.getThreads());
			this.sendGauge(id, "queue", stats.getQueue());
			this.sendGauge(id, "active", stats.getActive());
			this.sendGauge(id, "rejected", stats.getRejected());
			this.sendGauge(id, "largest", stats.getLargest());
			this.sendCount(id, "completed", stats.getCompleted());
		}
	}

	private void sendNodeTransportStats(TransportStats transportStats) {
		String type = this.getPrefix("transport");
		this.sendGauge(type, "server_open", transportStats.serverOpen());
		this.sendCount(type, "rx_count", transportStats.rxCount());
		this.sendCount(type, "rx_size_in_bytes", transportStats.rxSize().bytes());
		this.sendCount(type, "tx_count", transportStats.txCount());
		this.sendCount(type, "tx_size_in_bytes", transportStats.txSize().bytes());
	}

	private void sendNodeProcessStats(ProcessStats processStats) {
		String type = this.getPrefix("process");

		this.sendGauge(type, "open_file_descriptors", processStats.openFileDescriptors());

		if (processStats.cpu() != null) {
			this.sendGauge(type + ".cpu", "percent", processStats.cpu().percent());
			this.sendCount(type + ".cpu", "sys_in_millis", processStats.cpu().sys().millis());
			this.sendCount(type + ".cpu", "user_in_millis", processStats.cpu().user().millis());
			this.sendCount(type + ".cpu", "total_in_millis", processStats.cpu().total().millis());
		}

		if (processStats.mem() != null) {
			this.sendGauge(type + ".mem", "resident_in_bytes", processStats.mem().resident().bytes());
			this.sendGauge(type + ".mem", "share_in_bytes", processStats.mem().share().bytes());
			this.sendGauge(type + ".mem", "total_virtual_in_bytes", processStats.mem().totalVirtual().bytes());
		}
	}

	private void sendNodeOsStats(OsStats osStats) {
		String type = this.getPrefix("os");

		// Java client does not support doubles yet :(
		// https://github.com/tim-group/java-statsd-client/issues/19
		double[] loadAverage = osStats.getLoadAverage();
		if (loadAverage.length > 0) {
			this.sendGauge(type + ".load_average", "1m", (long) loadAverage[0]);
			this.sendGauge(type + ".load_average", "5m", (long) loadAverage[1]);
			this.sendGauge(type + ".load_average", "15m", (long) loadAverage[2]);
		}

		if (osStats.cpu() != null) {
			this.sendGauge(type + ".cpu", "sys", osStats.cpu().sys());
			this.sendGauge(type + ".cpu", "user", osStats.cpu().user());
			this.sendGauge(type + ".cpu", "idle", osStats.cpu().idle());
			this.sendGauge(type + ".cpu", "stolen", osStats.cpu().stolen());
		}

		if (osStats.mem() != null) {
			this.sendGauge(type + ".mem", "free_in_bytes", osStats.mem().free().bytes());
			this.sendGauge(type + ".mem", "used_in_bytes", osStats.mem().used().bytes());
			this.sendGauge(type + ".mem", "free_percent", osStats.mem().freePercent());
			this.sendGauge(type + ".mem", "used_percent", osStats.mem().usedPercent());
			this.sendGauge(type + ".mem", "actual_free_in_bytes", osStats.mem().actualFree().bytes());
			this.sendGauge(type + ".mem", "actual_used_in_bytes", osStats.mem().actualUsed().bytes());
		}

		if (osStats.swap() != null) {
			this.sendGauge(type + ".swap", "free_in_bytes", osStats.swap().free().bytes());
			this.sendGauge(type + ".swap", "used_in_bytes", osStats.swap().used().bytes());
		}
	}

	private void sendNodeNetworkStats(NetworkStats networkStats) {
		String type = this.getPrefix("network.tcp");
		NetworkStats.Tcp tcp = networkStats.tcp();

		// might be null, if sigar isnt loaded
		if (tcp != null) {
			this.sendCount(type, "active_opens", tcp.getActiveOpens());
			this.sendCount(type, "passive_opens", tcp.getPassiveOpens());
			this.sendGauge(type, "curr_estab", tcp.getCurrEstab());
			this.sendCount(type, "in_segs", tcp.inSegs());
			this.sendCount(type, "out_segs", tcp.outSegs());
			this.sendCount(type, "retrans_segs", tcp.retransSegs());
			this.sendCount(type, "estab_resets", tcp.estabResets());
			this.sendCount(type, "attempt_fails", tcp.attemptFails());
			this.sendCount(type, "in_errs", tcp.inErrs());
			this.sendCount(type, "out_rsts", tcp.outRsts());
		}
	}

	private void sendNodeJvmStats(JvmStats jvmStats) {
		String type = this.getPrefix("jvm");

		// mem
		this.sendGauge(type + ".mem", "heap_used_percent", jvmStats.mem().heapUsedPercent());
		this.sendGauge(type + ".mem", "heap_used_in_bytes", jvmStats.mem().heapUsed().bytes());
		this.sendGauge(type + ".mem", "heap_committed_in_bytes", jvmStats.mem().heapCommitted().bytes());
		this.sendGauge(type + ".mem", "non_heap_used_in_bytes", jvmStats.mem().nonHeapUsed().bytes());
		this.sendGauge(type + ".mem", "non_heap_committed_in_bytes", jvmStats.mem().nonHeapCommitted().bytes());
		for (JvmStats.MemoryPool memoryPool : jvmStats.mem()) {
			String memoryPoolType = type + ".mem.pools." + memoryPool.name();

			this.sendGauge(memoryPoolType, "max_in_bytes", memoryPool.max().bytes());
			this.sendGauge(memoryPoolType, "used_in_bytes", memoryPool.used().bytes());
			this.sendGauge(memoryPoolType, "peak_used_in_bytes", memoryPool.peakUsed().bytes());
			this.sendGauge(memoryPoolType, "peak_max_in_bytes", memoryPool.peakMax().bytes());
		}

		// threads
		this.sendGauge(type + ".threads", "count", jvmStats.threads().count());
		this.sendGauge(type + ".threads", "peak_count", jvmStats.threads().peakCount());

		// garbage collectors
		for (JvmStats.GarbageCollector collector : jvmStats.gc()) {
			String id = type + ".gc.collectors." + collector.name();

			this.sendCount(id, "collection_count", collector.collectionCount());
			this.sendCount(id, "collection_time_in_millis", collector.collectionTime().millis());
		}

		// TODO: buffer pools
	}

	private void sendNodeHttpStats(HttpStats httpStats) {
		String type = this.getPrefix("http");
		this.sendGauge(type, "current_open", httpStats.getServerOpen());
		this.sendCount(type, "total_opened", httpStats.getTotalOpen());
	}

	private void sendNodeFsStats(FsStats fs) {
		// Send total
		String type = this.getPrefix("fs");
		this.sendNodeFsStatsInfo(type + ".total", fs.total());

		// Maybe send details
		if (this.statsdReportFsDetails) {
			Iterator<FsStats.Info> infoIterator = fs.iterator();
			while (infoIterator.hasNext()) {
				FsStats.Info info = infoIterator.next();
				this.sendNodeFsStatsInfo(type + ".data", info);
			}
		}
	}

	private void sendNodeFsStatsInfo(String type, FsStats.Info info) {
		// Construct detailed path
		String typeAppend = "";
		if (info.getPath() != null)
			typeAppend += "." + info.getPath();
		if (info.getMount() != null)
			typeAppend += "." + info.getMount();
		if (info.getDev() != null)
			typeAppend += "." + info.getDev();

		if (info.getAvailable().bytes() != -1)
			this.sendGauge(type + typeAppend, "available_in_bytes", info.getAvailable().bytes());
		if (info.getTotal().bytes() != -1)
			this.sendGauge(type + typeAppend, "total_in_bytes", info.getTotal().bytes());
		if (info.getFree().bytes() != -1)
			this.sendGauge(type + typeAppend, "free_in_bytes", info.getFree().bytes());

		// disk_io_op is sum of reads and writes (use graphite functions)
		if (info.getDiskReads() != -1)
			this.sendCount(type + typeAppend, "disk_reads", info.getDiskReads());
		if (info.getDiskWrites() != -1)
			this.sendCount(type + typeAppend, "disk_writes", info.getDiskWrites());

		// disk_io_size_in_bytes is sum of reads and writes (use graphite functions)
		if (info.getDiskReadSizeInBytes() != -1)
			this.sendCount(type + typeAppend, "disk_read_size_in_bytes", info.getDiskReadSizeInBytes());
		if (info.getDiskWriteSizeInBytes() != -1)
			this.sendCount(type + typeAppend, "disk_write_size_in_bytes", info.getDiskWriteSizeInBytes());

		/** TODO: Find out if these stats are useful.
		if (info.getDiskQueue() != -1)
			this.sendGauge(type + typeAppend, "disk_queue", (long) info.getDiskQueue());
		if (info.getDiskServiceTime() != -1)
			this.sendGauge(type + typeAppend, "disk_service_time", (long) info.getDiskServiceTime());
		*/
	}

	private String getPrefix(String prefix) {
		return this.buildMetricName( "node." + this.nodeName + "." + prefix );
	}
}
