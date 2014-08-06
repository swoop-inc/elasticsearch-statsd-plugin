package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import com.timgroup.statsd.StatsDClient;

public class StatsdReporter {

	private static final String		DEFAULT_JOINER	= ".";
	private static final ESLogger	logger			= ESLoggerFactory.getLogger(StatsdReporter.class.getName());
	private List<IndexShard>		indexShards;
	private NodeStats				nodeStats;
	private final NodeIndicesStats	nodeIndicesStats;
	private final StatsDClient		statsdClient;
	private String					joiner			= DEFAULT_JOINER;

	public StatsdReporter(NodeIndicesStats nodeIndicesStats, List<IndexShard> indexShards, NodeStats nodeStats,
			StatsDClient statsdClient)
	{

		this.indexShards = indexShards;
		this.nodeStats = nodeStats;
		this.nodeIndicesStats = nodeIndicesStats;
		this.statsdClient = statsdClient;
	}

	public void run()
	{
		try {
			this.sendNodeIndicesStats();
			this.sendIndexShardStats();
			this.sendNodeStats();
		}
		catch (Exception e) {
			this.logException(e);
		}
	}

	private void sendNodeStats()
	{
		this.sendNodeFsStats(this.nodeStats.getFs());
		this.sendNodeHttpStats(this.nodeStats.getHttp());
		this.sendNodeJvmStats(this.nodeStats.getJvm());
		this.sendNodeNetworkStats(this.nodeStats.getNetwork());
		this.sendNodeOsStats(this.nodeStats.getOs());
		this.sendNodeProcessStats(this.nodeStats.getProcess());
		this.sendNodeTransportStats(this.nodeStats.getTransport());
		this.sendNodeThreadPoolStats(this.nodeStats.getThreadPool());
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

	private void sendIndexShardStats()
	{
		for (IndexShard indexShard : this.indexShards) {
			String type = this.buildMetricName("indexes.") + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
			this.sendIndexShardStats(type, indexShard);
		}
	}

	private void sendIndexShardStats(String type, IndexShard indexShard)
	{
		this.sendSearchStats(type + ".search", indexShard.searchStats());
		this.sendGetStats(type + ".get", indexShard.getStats());
		this.sendDocsStats(type + ".docs", indexShard.docStats());
		this.sendRefreshStats(type + ".refresh", indexShard.refreshStats());
		this.sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
		this.sendMergeStats(type + ".merge", indexShard.mergeStats());
		this.sendWarmerStats(type + ".warmer", indexShard.warmerStats());
		this.sendStoreStats(type + ".store", indexShard.storeStats());
	}

	private void sendStoreStats(String type, StoreStats storeStats)
	{
		this.sendGauge(type, "sizeInBytes", storeStats.sizeInBytes());
		this.sendGauge(type, "throttleTimeInNanos", storeStats.throttleTime().getNanos());
	}

	private void sendWarmerStats(String type, WarmerStats warmerStats)
	{
		this.sendGauge(type, "current", warmerStats.current());
		this.sendGauge(type, "total", warmerStats.total());
		this.sendTime(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
	}

	private void sendMergeStats(String type, MergeStats mergeStats)
	{
		this.sendGauge(type, "total", mergeStats.getTotal());
		this.sendTime(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
		this.sendGauge(type, "totalNumDocs", mergeStats.getTotalNumDocs());
		this.sendGauge(type, "current", mergeStats.getCurrent());
		this.sendGauge(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
		this.sendGauge(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
	}

	private void sendNodeIndicesStats()
	{
		String type = this.buildMetricName("node");
		this.sendFilterCacheStats(type + ".filtercache", this.nodeIndicesStats.getFilterCache());
		this.sendIdCacheStats(type + ".idcache", this.nodeIndicesStats.getIdCache());
		this.sendDocsStats(type + ".docs", this.nodeIndicesStats.getDocs());
		this.sendFlushStats(type + ".flush", this.nodeIndicesStats.getFlush());
		this.sendGetStats(type + ".get", this.nodeIndicesStats.getGet());
		this.sendIndexingStats(type + ".indexing", this.nodeIndicesStats.getIndexing());
		this.sendRefreshStats(type + ".refresh", this.nodeIndicesStats.getRefresh());
		this.sendSearchStats(type + ".search", this.nodeIndicesStats.getSearch());
	}

	private void sendSearchStats(String type, SearchStats searchStats)
	{
		SearchStats.Stats totalSearchStats = searchStats.getTotal();
		this.sendSearchStatsStats(type + "._all", totalSearchStats);

		if (searchStats.getGroupStats() != null) {
			for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
				this.sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
			}
		}
	}

	private void sendSearchStatsStats(String group, SearchStats.Stats searchStats)
	{
		String type = this.buildMetricName("search.stats.") + group;
		this.sendCount(type, "queryCount", searchStats.getQueryCount());
		this.sendCount(type, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
		this.sendGauge(type, "queryCurrent", searchStats.getQueryCurrent());
		this.sendCount(type, "fetchCount", searchStats.getFetchCount());
		this.sendCount(type, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
		this.sendGauge(type, "fetchCurrent", searchStats.getFetchCurrent());
	}

	private void sendRefreshStats(String type, RefreshStats refreshStats)
	{
		this.sendCount(type, "total", refreshStats.getTotal());
		this.sendCount(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
	}

	private void sendIndexingStats(String type, IndexingStats indexingStats)
	{
		IndexingStats.Stats totalStats = indexingStats.getTotal();
		this.sendStats(type + "._all", totalStats);

		Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
		if (typeStats != null) {
			for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
				this.sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
			}
		}
	}

	private void sendStats(String type, IndexingStats.Stats stats)
	{
		this.sendCount(type, "indexCount", stats.getIndexCount());
		this.sendCount(type, "indexTimeInMillis", stats.getIndexTimeInMillis());
		this.sendGauge(type, "indexCurrent", stats.getIndexCount());
		this.sendCount(type, "deleteCount", stats.getDeleteCount());
		this.sendCount(type, "deleteTimeInMillis", stats.getDeleteTimeInMillis());
		this.sendGauge(type, "deleteCurrent", stats.getDeleteCurrent());
	}

	private void sendGetStats(String type, GetStats getStats)
	{
		this.sendCount(type, "existsCount", getStats.getExistsCount());
		this.sendCount(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
		this.sendCount(type, "missingCount", getStats.getMissingCount());
		this.sendCount(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
		this.sendGauge(type, "current", getStats.current());
	}

	private void sendFlushStats(String type, FlushStats flush)
	{
		this.sendCount(type, "total", flush.getTotal());
		this.sendCount(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
	}

	private void sendDocsStats(String name, DocsStats docsStats)
	{
		this.sendCount(name, "count", docsStats.getCount());
		this.sendCount(name, "deleted", docsStats.getDeleted());
	}

	private void sendIdCacheStats(String name, IdCacheStats idCache)
	{
		this.sendGauge(name, "memorySizeInBytes", idCache.getMemorySizeInBytes());
	}

	private void sendFilterCacheStats(String name, FilterCacheStats filterCache)
	{
		this.sendGauge(name, "memorySizeInBytes", filterCache.getMemorySizeInBytes());
		this.sendGauge(name, "evictions", filterCache.getEvictions());
	}

	protected void sendGauge(String name, String valueName, long value)
	{
		this.statsdClient.gauge(this.join(name, valueName), (int) value);
	}

	protected void sendCount(String name, String valueName, long value)
	{
		this.statsdClient.count(this.join(name, valueName), (int) value);
	}

	protected void sendTime(String name, String valueName, long value)
	{
		this.statsdClient.time(this.join(name, valueName), (int) value);
	}

	protected String sanitizeString(String s)
	{
		return s.replace(' ', '-');
	}

	protected String buildMetricName(String name)
	{
		return this.sanitizeString(name);
	}

	private String join(String... parts)
	{
		if (parts == null) {
			return null;
		}
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < parts.length; i++) {
			builder.append(parts[i]);
			if (i < parts.length - 1) {
				builder.append(this.joiner);
			}
		}
		return builder.toString();
	}

	private void logException(Exception e)
	{
		if (logger.isDebugEnabled()) {
			logger.debug("Error writing to Statsd", e);
		}
		else {
			logger.warn("Error writing to Statsd: {}", e.getMessage());
		}
	}
}
