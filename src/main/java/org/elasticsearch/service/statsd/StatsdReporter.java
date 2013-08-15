package org.elasticsearch.service.statsd;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

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
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StatsdReporter {

	private static final String DEFAULT_JOINER = ".";
    private static final ESLogger logger = ESLoggerFactory.getLogger(StatsdReporter.class.getName());
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final NodeIndicesStats nodeIndicesStats;
    private final StatsDClient statsdClient;
    private String joiner = DEFAULT_JOINER;

    public StatsdReporter(String host, int port, String prefix, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats) {
    	
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.nodeIndicesStats = nodeIndicesStats;
        this.statsdClient = new NonBlockingStatsDClient(prefix, host, port);
    }

    public void run() {
        try {
            sendNodeIndicesStats();
            sendIndexShardStats();
            sendNodeStats();
        } catch (Exception e) {
            logException(e);
        }
    }

    private void sendNodeStats() {
        sendNodeFsStats(nodeStats.getFs());
        sendNodeHttpStats(nodeStats.getHttp());
        sendNodeJvmStats(nodeStats.getJvm());
        sendNodeNetworkStats(nodeStats.getNetwork());
        sendNodeOsStats(nodeStats.getOs());
        sendNodeProcessStats(nodeStats.getProcess());
        sendNodeTransportStats(nodeStats.getTransport());
        sendNodeThreadPoolStats(nodeStats.getThreadPool());
    }

    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
        String type = buildMetricName("node.threadpool");
        Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
        while (statsIterator.hasNext()) {
            ThreadPoolStats.Stats stats = statsIterator.next();
            String id = type + "." + stats.getName();

            sendGauge(id, "threads", stats.getThreads());
            sendGauge(id, "queue", stats.getQueue());
            sendGauge(id, "active", stats.getActive());
            sendGauge(id, "rejected", stats.getRejected());
            sendGauge(id, "largest", stats.getLargest());
            sendGauge(id, "completed", stats.getCompleted());
        }
    }

    private void sendNodeTransportStats(TransportStats transportStats) {
        String type = buildMetricName("node.transport");
        sendGauge(type, "serverOpen", transportStats.serverOpen());
        sendCount(type, "rxCount", transportStats.rxCount());
        sendCount(type, "rxSizeBytes", transportStats.rxSize().bytes());
        sendCount(type, "txCount", transportStats.txCount());
        sendCount(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) {
        String type = buildMetricName("node.process");

        sendGauge(type, "openFileDescriptors", processStats.openFileDescriptors());
        if (processStats.cpu() != null) {
            sendGauge(type + ".cpu", "percent", processStats.cpu().percent());
            sendGauge(type + ".cpu", "sysSeconds", processStats.cpu().sys().seconds());
            sendGauge(type + ".cpu", "totalSeconds", processStats.cpu().total().seconds());
            sendGauge(type + ".cpu", "userSeconds", processStats.cpu().user().seconds());
        }

        if (processStats.mem() != null) {
            sendGauge(type + ".mem", "totalVirtual", processStats.mem().totalVirtual().bytes());
            sendGauge(type + ".mem", "resident", processStats.mem().resident().bytes());
            sendGauge(type + ".mem", "share", processStats.mem().share().bytes());
        }
    }

    private void sendNodeOsStats(OsStats osStats) {
        String type = buildMetricName("node.os");

        if (osStats.cpu() != null) {
            sendGauge(type + ".cpu", "sys", osStats.cpu().sys());
            sendGauge(type + ".cpu", "idle", osStats.cpu().idle());
            sendGauge(type + ".cpu", "user", osStats.cpu().user());
        }

        if (osStats.mem() != null) {
            sendGauge(type + ".mem", "freeBytes", osStats.mem().free().bytes());
            sendGauge(type + ".mem", "usedBytes", osStats.mem().used().bytes());
            sendGauge(type + ".mem", "freePercent", osStats.mem().freePercent());
            sendGauge(type + ".mem", "usedPercent", osStats.mem().usedPercent());
            sendGauge(type + ".mem", "actualFreeBytes", osStats.mem().actualFree().bytes());
            sendGauge(type + ".mem", "actualUsedBytes", osStats.mem().actualUsed().bytes());
        }

        if (osStats.swap() != null) {
            sendGauge(type + ".swap", "freeBytes", osStats.swap().free().bytes());
            sendGauge(type + ".swap", "usedBytes", osStats.swap().used().bytes());
        }
    }

    private void sendNodeNetworkStats(NetworkStats networkStats) {
        String type = buildMetricName("node.network.tcp");
        NetworkStats.Tcp tcp = networkStats.tcp();

        // might be null, if sigar isnt loaded
        if (tcp != null) {
            sendGauge(type, "activeOpens", tcp.activeOpens());
            sendGauge(type, "passiveOpens", tcp.passiveOpens());
            sendGauge(type, "attemptFails", tcp.attemptFails());
            sendGauge(type, "estabResets", tcp.estabResets());
            sendGauge(type, "currEstab", tcp.currEstab());
            sendGauge(type, "inSegs", tcp.inSegs());
            sendGauge(type, "outSegs", tcp.outSegs());
            sendGauge(type, "retransSegs", tcp.retransSegs());
            sendGauge(type, "inErrs", tcp.inErrs());
            sendGauge(type, "outRsts", tcp.outRsts());
        }
    }

    private void sendNodeJvmStats(JvmStats jvmStats) {
        String type = buildMetricName("node.jvm");
        sendGauge(type, "uptime", jvmStats.uptime().seconds());

        // mem
        sendGauge(type + ".mem", "heapCommitted", jvmStats.mem().heapCommitted().bytes());
        sendGauge(type + ".mem", "heapUsed", jvmStats.mem().heapUsed().bytes());
        sendGauge(type + ".mem", "nonHeapCommitted", jvmStats.mem().nonHeapCommitted().bytes());
        sendGauge(type + ".mem", "nonHeapUsed", jvmStats.mem().nonHeapUsed().bytes());

        Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.mem().iterator();
        while (memoryPoolIterator.hasNext()) {
            JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
            String memoryPoolType = type + ".mem.pool." + memoryPool.name();

            sendGauge(memoryPoolType, "max", memoryPool.max().bytes());
            sendGauge(memoryPoolType, "used", memoryPool.used().bytes());
            sendGauge(memoryPoolType, "peakUsed", memoryPool.peakUsed().bytes());
            sendGauge(memoryPoolType, "peakMax", memoryPool.peakMax().bytes());
        }

        // threads
        sendGauge(type + ".threads", "count", jvmStats.threads().count());
        sendGauge(type + ".threads", "peakCount", jvmStats.threads().peakCount());

        // garbage collectors
        sendCount(type + ".gc", "collectionCount", jvmStats.gc().collectionCount());
        sendTime(type + ".gc", "collectionTimeSeconds", jvmStats.gc().collectionTime().seconds());
        for (JvmStats.GarbageCollector collector : jvmStats.gc().collectors()) {
            String id = type + ".gc." + collector.name();
            sendCount(id, "collectionCount", collector.collectionCount());
            sendTime(id, "collectionTimeSeconds", collector.collectionTime().seconds());

            JvmStats.GarbageCollector.LastGc lastGc = collector.lastGc();
            String lastGcType = type + ".lastGc";
            if (lastGc != null) {
                sendTime(lastGcType, "time", lastGc.endTime() - lastGc.startTime());
                sendGauge(lastGcType, "max", lastGc.max().bytes());
                sendGauge(lastGcType, "beforeUsed", lastGc.beforeUsed().bytes());
                sendGauge(lastGcType, "afterUsed", lastGc.afterUsed().bytes());
                sendGauge(lastGcType, "durationSeconds", lastGc.duration().seconds());
            }
        }
    }

    private void sendNodeHttpStats(HttpStats httpStats) {
        String type = buildMetricName("node.http");
        sendGauge(type, "serverOpen", httpStats.getServerOpen());
        sendGauge(type, "totalOpen", httpStats.getTotalOpen());
    }

    private void sendNodeFsStats(FsStats fs) {
        Iterator<FsStats.Info> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = buildMetricName("node.fs") + i;
            FsStats.Info info = infoIterator.next();
            sendGauge(type, "available", info.getAvailable().bytes());
            sendGauge(type, "total", info.getTotal().bytes());
            sendGauge(type, "free", info.getFree().bytes());
            sendCount(type, "diskReads", info.getDiskReads());
            sendCount(type, "diskReadsInBytes", info.getDiskReadSizeInBytes());
            sendCount(type, "diskWrites", info.getDiskWrites());
            sendCount(type, "diskWritesInBytes", info.getDiskWriteSizeInBytes());
            sendGauge(type, "diskQueue", (long) info.getDiskQueue());
            sendGauge(type, "diskService", (long) info.getDiskServiceTime());
            i++;
        }
    }

    private void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String type = buildMetricName("indexes.") + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
            sendIndexShardStats(type, indexShard);
        }
    }

    private void sendIndexShardStats(String type, IndexShard indexShard) {
        sendSearchStats(type + ".search", indexShard.searchStats());
        sendGetStats(type + ".get", indexShard.getStats());
        sendDocsStats(type + ".docs", indexShard.docStats());
        sendRefreshStats(type + ".refresh", indexShard.refreshStats());
        sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
        sendMergeStats(type + ".merge", indexShard.mergeStats());
        sendWarmerStats(type + ".warmer", indexShard.warmerStats());
        sendStoreStats(type + ".store", indexShard.storeStats());
    }

    private void sendStoreStats(String type, StoreStats storeStats) {
        sendGauge(type, "sizeInBytes", storeStats.sizeInBytes());
        sendGauge(type, "throttleTimeInNanos", storeStats.throttleTime().getNanos());
    }

    private void sendWarmerStats(String type, WarmerStats warmerStats) {
    	sendGauge(type, "current", warmerStats.current());
    	sendGauge(type, "total", warmerStats.total());
    	sendTime(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
    }

    private void sendMergeStats(String type, MergeStats mergeStats) {
    	sendGauge(type, "total", mergeStats.getTotal());
        sendTime(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
        sendGauge(type, "totalNumDocs", mergeStats.getTotalNumDocs());
        sendGauge(type, "current", mergeStats.getCurrent());
        sendGauge(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
        sendGauge(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
    }

    private void sendNodeIndicesStats() {
        String type = buildMetricName("node");
        sendFilterCacheStats(type + ".filtercache", nodeIndicesStats.getFilterCache());
        sendIdCacheStats(type + ".idcache", nodeIndicesStats.getIdCache());
        sendDocsStats(type + ".docs", nodeIndicesStats.getDocs());
        sendFlushStats(type + ".flush", nodeIndicesStats.getFlush());
        sendGetStats(type + ".get", nodeIndicesStats.getGet());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.getIndexing());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.getRefresh());
        sendSearchStats(type + ".search", nodeIndicesStats.getSearch());
    }

    private void sendSearchStats(String type, SearchStats searchStats) {
        SearchStats.Stats totalSearchStats = searchStats.getTotal();
        sendSearchStatsStats(type + "._all", totalSearchStats);

        if (searchStats.getGroupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String group, SearchStats.Stats searchStats) {
        String type = buildMetricName("search.stats.") + group;
        sendCount(type, "queryCount", searchStats.getQueryCount());
        sendCount(type, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
        sendGauge(type, "queryCurrent", searchStats.getQueryCurrent());
        sendCount(type, "fetchCount", searchStats.getFetchCount());
        sendCount(type, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
        sendGauge(type, "fetchCurrent", searchStats.getFetchCurrent());
    }

    private void sendRefreshStats(String type, RefreshStats refreshStats) {
        sendCount(type, "total", refreshStats.getTotal());
        sendCount(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
    }

    private void sendIndexingStats(String type, IndexingStats indexingStats) {
        IndexingStats.Stats totalStats = indexingStats.getTotal();
        sendStats(type + "._all", totalStats);

        Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
        if (typeStats != null) {
            for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendStats(String type, IndexingStats.Stats stats) {
        sendCount(type, "indexCount", stats.getIndexCount());
        sendCount(type, "indexTimeInMillis", stats.getIndexTimeInMillis());
        sendGauge(type, "indexCurrent", stats.getIndexCount());
        sendCount(type, "deleteCount", stats.getDeleteCount());
        sendCount(type, "deleteTimeInMillis", stats.getDeleteTimeInMillis());
        sendGauge(type, "deleteCurrent", stats.getDeleteCurrent());
    }

    private void sendGetStats(String type, GetStats getStats) {
        sendCount(type, "existsCount", getStats.getExistsCount());
        sendCount(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
        sendCount(type, "missingCount", getStats.getMissingCount());
        sendCount(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
        sendGauge(type, "current", getStats.current());
    }

    private void sendFlushStats(String type, FlushStats flush) {
        sendCount(type, "total", flush.getTotal());
        sendCount(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) {
        sendCount(name, "count", docsStats.getCount());
        sendCount(name, "deleted", docsStats.getDeleted());
    }

    private void sendIdCacheStats(String name, IdCacheStats idCache) {
        sendGauge(name, "memorySizeInBytes", idCache.getMemorySizeInBytes());
    }

    private void sendFilterCacheStats(String name, FilterCacheStats filterCache) {
    	sendGauge(name, "memorySizeInBytes", filterCache.getMemorySizeInBytes());
    	sendGauge(name, "evictions", filterCache.getEvictions());
    }
    
    protected void sendGauge(String name, String valueName, long value) {
    	statsdClient.gauge(join(name, valueName), (int) value);
    }
    
    protected void sendCount(String name, String valueName, long value) {
    	statsdClient.count(join(name, valueName), (int) value);
    }
    
    protected void sendTime(String name, String valueName, long value) {
    	statsdClient.time(join(name, valueName), (int) value);
    }
    
    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    protected String buildMetricName(String name) {
        return sanitizeString(name);
    }
    
    private String join(String...parts) {
    	if (parts == null) return null;
    	StringBuilder builder = new StringBuilder();
    	for (int i = 0; i < parts.length; i++) {
    		builder.append(parts[i]);
    		if (i < parts.length - 1) {
    			builder.append(joiner);
    		}
		}
    	return builder.toString();
    }

    private void logException(Exception e) {
        if (logger.isDebugEnabled()) {
            logger.debug("Error writing to Statsd", e);
        } else {
            logger.warn("Error writing to Statsd: {}", e.getMessage());
        }
    }
}
