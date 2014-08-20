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
		String prefix = this.getPrefix("thread_pool");
		Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
		while (statsIterator.hasNext()) {
			ThreadPoolStats.Stats stats = statsIterator.next();
			String threadPoolType = prefix + "." + stats.getName();

			this.sendGauge(threadPoolType, "threads", stats.getThreads());
			this.sendGauge(threadPoolType, "queue", stats.getQueue());
			this.sendGauge(threadPoolType, "active", stats.getActive());
			this.sendGauge(threadPoolType, "rejected", stats.getRejected());
			this.sendGauge(threadPoolType, "largest", stats.getLargest());
			this.sendGauge(threadPoolType, "completed", stats.getCompleted());
		}
	}

	private void sendNodeTransportStats(TransportStats transportStats) {
		String prefix = this.getPrefix("transport");
		this.sendGauge(prefix, "server_open", transportStats.serverOpen());
		this.sendGauge(prefix, "rx_count", transportStats.rxCount());
		this.sendGauge(prefix, "rx_size_in_bytes", transportStats.rxSize().bytes());
		this.sendGauge(prefix, "tx_count", transportStats.txCount());
		this.sendGauge(prefix, "tx_size_in_bytes", transportStats.txSize().bytes());
	}

	private void sendNodeProcessStats(ProcessStats processStats) {
		String prefix = this.getPrefix("process");

		this.sendGauge(prefix, "open_file_descriptors", processStats.openFileDescriptors());

		if (processStats.cpu() != null) {
			this.sendGauge(prefix + ".cpu", "percent", processStats.cpu().percent());
			this.sendGauge(prefix + ".cpu", "sys_in_millis", processStats.cpu().sys().millis());
			this.sendGauge(prefix + ".cpu", "user_in_millis", processStats.cpu().user().millis());
			this.sendGauge(prefix + ".cpu", "total_in_millis", processStats.cpu().total().millis());
		}

		if (processStats.mem() != null) {
			this.sendGauge(prefix + ".mem", "resident_in_bytes", processStats.mem().resident().bytes());
			this.sendGauge(prefix + ".mem", "share_in_bytes", processStats.mem().share().bytes());
			this.sendGauge(prefix + ".mem", "total_virtual_in_bytes", processStats.mem().totalVirtual().bytes());
		}
	}

	private void sendNodeOsStats(OsStats osStats) {
		String prefix = this.getPrefix("os");

		double[] loadAverage = osStats.getLoadAverage();
		if (loadAverage.length > 0) {
			this.sendGauge(prefix + ".load_average", "1m", loadAverage[0]);
			this.sendGauge(prefix + ".load_average", "5m", loadAverage[1]);
			this.sendGauge(prefix + ".load_average", "15m", loadAverage[2]);
		}

		if (osStats.cpu() != null) {
			this.sendGauge(prefix + ".cpu", "sys", osStats.cpu().sys());
			this.sendGauge(prefix + ".cpu", "user", osStats.cpu().user());
			this.sendGauge(prefix + ".cpu", "idle", osStats.cpu().idle());
			this.sendGauge(prefix + ".cpu", "stolen", osStats.cpu().stolen());
		}

		if (osStats.mem() != null) {
			this.sendGauge(prefix + ".mem", "free_in_bytes", osStats.mem().free().bytes());
			this.sendGauge(prefix + ".mem", "used_in_bytes", osStats.mem().used().bytes());
			this.sendGauge(prefix + ".mem", "free_percent", osStats.mem().freePercent());
			this.sendGauge(prefix + ".mem", "used_percent", osStats.mem().usedPercent());
			this.sendGauge(prefix + ".mem", "actual_free_in_bytes", osStats.mem().actualFree().bytes());
			this.sendGauge(prefix + ".mem", "actual_used_in_bytes", osStats.mem().actualUsed().bytes());
		}

		if (osStats.swap() != null) {
			this.sendGauge(prefix + ".swap", "free_in_bytes", osStats.swap().free().bytes());
			this.sendGauge(prefix + ".swap", "used_in_bytes", osStats.swap().used().bytes());
		}
	}

	private void sendNodeNetworkStats(NetworkStats networkStats) {
		String prefix = this.getPrefix("network.tcp");
		NetworkStats.Tcp tcp = networkStats.tcp();

		// might be null, if sigar isnt loaded
		if (tcp != null) {
			this.sendGauge(prefix, "active_opens", tcp.getActiveOpens());
			this.sendGauge(prefix, "passive_opens", tcp.getPassiveOpens());
			this.sendGauge(prefix, "curr_estab", tcp.getCurrEstab());
			this.sendGauge(prefix, "in_segs", tcp.inSegs());
			this.sendGauge(prefix, "out_segs", tcp.outSegs());
			this.sendGauge(prefix, "retrans_segs", tcp.retransSegs());
			this.sendGauge(prefix, "estab_resets", tcp.estabResets());
			this.sendGauge(prefix, "attempt_fails", tcp.attemptFails());
			this.sendGauge(prefix, "in_errs", tcp.inErrs());
			this.sendGauge(prefix, "out_rsts", tcp.outRsts());
		}
	}

	private void sendNodeJvmStats(JvmStats jvmStats) {
		String prefix = this.getPrefix("jvm");

		// mem
		this.sendGauge(prefix + ".mem", "heap_used_percent", jvmStats.mem().heapUsedPercent());
		this.sendGauge(prefix + ".mem", "heap_used_in_bytes", jvmStats.mem().heapUsed().bytes());
		this.sendGauge(prefix + ".mem", "heap_committed_in_bytes", jvmStats.mem().heapCommitted().bytes());
		this.sendGauge(prefix + ".mem", "non_heap_used_in_bytes", jvmStats.mem().nonHeapUsed().bytes());
		this.sendGauge(prefix + ".mem", "non_heap_committed_in_bytes", jvmStats.mem().nonHeapCommitted().bytes());
		for (JvmStats.MemoryPool memoryPool : jvmStats.mem()) {
			String memoryPoolType = prefix + ".mem.pools." + memoryPool.name();

			this.sendGauge(memoryPoolType, "max_in_bytes", memoryPool.max().bytes());
			this.sendGauge(memoryPoolType, "used_in_bytes", memoryPool.used().bytes());
			this.sendGauge(memoryPoolType, "peak_used_in_bytes", memoryPool.peakUsed().bytes());
			this.sendGauge(memoryPoolType, "peak_max_in_bytes", memoryPool.peakMax().bytes());
		}

		// threads
		this.sendGauge(prefix + ".threads", "count", jvmStats.threads().count());
		this.sendGauge(prefix + ".threads", "peak_count", jvmStats.threads().peakCount());

		// garbage collectors
		for (JvmStats.GarbageCollector collector : jvmStats.gc()) {
			String gcCollectorType = prefix + ".gc.collectors." + collector.name();

			this.sendGauge(gcCollectorType, "collection_count", collector.collectionCount());
			this.sendGauge(gcCollectorType, "collection_time_in_millis", collector.collectionTime().millis());
		}

		// TODO: buffer pools
	}

	private void sendNodeHttpStats(HttpStats httpStats) {
		String prefix = this.getPrefix("http");
		this.sendGauge(prefix, "current_open", httpStats.getServerOpen());
		this.sendGauge(prefix, "total_opened", httpStats.getTotalOpen());
	}

	private void sendNodeFsStats(FsStats fs) {
		// Send total
		String prefix = this.getPrefix("fs");
		this.sendNodeFsStatsInfo(prefix + ".total", fs.total());

		// Maybe send details
		if (this.statsdReportFsDetails) {
			Iterator<FsStats.Info> infoIterator = fs.iterator();
			while (infoIterator.hasNext()) {
				FsStats.Info info = infoIterator.next();
				this.sendNodeFsStatsInfo(prefix + ".data", info);
			}
		}
	}

	private void sendNodeFsStatsInfo(String prefix, FsStats.Info info) {
		// Construct detailed path
		String prefixAppend = "";
		if (info.getPath() != null)
			prefixAppend += "." + info.getPath();
		if (info.getMount() != null)
			prefixAppend += "." + info.getMount();
		if (info.getDev() != null)
			prefixAppend += "." + info.getDev();

		if (info.getAvailable().bytes() != -1)
			this.sendGauge(prefix + prefixAppend, "available_in_bytes", info.getAvailable().bytes());
		if (info.getTotal().bytes() != -1)
			this.sendGauge(prefix + prefixAppend, "total_in_bytes", info.getTotal().bytes());
		if (info.getFree().bytes() != -1)
			this.sendGauge(prefix + prefixAppend, "free_in_bytes", info.getFree().bytes());

		// disk_io_op is sum of reads and writes (use graphite functions)
		if (info.getDiskReads() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_reads", info.getDiskReads());
		if (info.getDiskWrites() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_writes", info.getDiskWrites());

		// disk_io_size_in_bytes is sum of reads and writes (use graphite functions)
		if (info.getDiskReadSizeInBytes() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_read_size_in_bytes", info.getDiskReadSizeInBytes());
		if (info.getDiskWriteSizeInBytes() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_write_size_in_bytes", info.getDiskWriteSizeInBytes());

		/** TODO: Find out if these stats are useful.
		if (info.getDiskQueue() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_queue", (long) info.getDiskQueue());
		if (info.getDiskServiceTime() != -1)
			this.sendGauge(prefix + prefixAppend, "disk_service_time", (long) info.getDiskServiceTime());
		*/
	}

	private String getPrefix(String prefix) {
		return this.buildMetricName( "node." + this.nodeName + "." + prefix );
	}
}
