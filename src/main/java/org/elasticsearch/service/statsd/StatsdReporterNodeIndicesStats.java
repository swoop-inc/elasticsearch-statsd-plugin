package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.percolator.stats.PercolateStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.index.engine.SegmentsStats;

public class StatsdReporterNodeIndicesStats extends StatsdReporter {

	private final NodeIndicesStats nodeIndicesStats;

	public StatsdReporterNodeIndicesStats(NodeIndicesStats nodeIndicesStats) {
		this.nodeIndicesStats = nodeIndicesStats;
	}

	public void run() {
		try {
			String type = this.buildMetricName("node.indices");
			this.sendDocsStats(type + ".docs", this.nodeIndicesStats.getDocs());
			this.sendStoreStats(type + ".store", this.nodeIndicesStats.getStore());
			this.sendIndexingStats(type + ".indexing", this.nodeIndicesStats.getIndexing());
			this.sendGetStats(type + ".get", this.nodeIndicesStats.getGet());
			this.sendSearchStats(type + ".search", this.nodeIndicesStats.getSearch());
			this.sendMergeStats(type + ".merges", this.nodeIndicesStats.getMerge());
			this.sendRefreshStats(type + ".refresh", this.nodeIndicesStats.getRefresh());
			this.sendFlushStats(type + ".flush", this.nodeIndicesStats.getFlush());
			this.sendFilterCacheStats(type + ".filter_cache", this.nodeIndicesStats.getFilterCache());
			this.sendIdCacheStats(type + ".id_cache", this.nodeIndicesStats.getIdCache());
			this.sendFielddataCacheStats(type + ".fielddata", this.nodeIndicesStats.getFieldData());
			this.sendPercolateStats(type + ".percolate", this.nodeIndicesStats.getPercolate());
			this.sendCompletionStats(type + ".completion", this.nodeIndicesStats.getCompletion());
			this.sendSegmentsStats(type + ".segments", this.nodeIndicesStats.getSegments());
		}
		catch (Exception e) {
			this.logException(e);
		}
	}

	private void sendDocsStats(String name, DocsStats docsStats) {
		this.sendCount(name, "count", docsStats.getCount());
		this.sendCount(name, "deleted", docsStats.getDeleted());
	}

	private void sendStoreStats(String name, StoreStats storeStats) {
		this.sendCount(name, "size_in_bytes", storeStats.sizeInBytes());
		this.sendCount(name, "throttle_time_in_millis", storeStats.getThrottleTime().millis());
	}

	private void sendIndexingStats(String type, IndexingStats indexingStats) {
		IndexingStats.Stats totalStats = indexingStats.getTotal();
		this.sendIndexingStatsStats(type, totalStats);

		// TODO: Maybe print out stats to shards level?
	}

	private void sendGetStats(String type, GetStats getStats) {
		this.sendCount(type, "total", getStats.getCount());
		this.sendCount(type, "time_in_millis", getStats.getTimeInMillis());
		this.sendCount(type, "exists_total", getStats.getExistsCount());
		this.sendCount(type, "exists_time_in_millis", getStats.getExistsTimeInMillis());
		this.sendCount(type, "missing_total", getStats.getMissingCount());
		this.sendCount(type, "missing_time_in_millis", getStats.getMissingTimeInMillis());
		this.sendGauge(type, "current", getStats.current());
	}

	private void sendSearchStats(String type, SearchStats searchStats) {
		SearchStats.Stats totalSearchStats = searchStats.getTotal();
		this.sendSearchStatsStats(type, totalSearchStats);

		// TODO: Maybe print out stats to shards level?
	}

	private void sendMergeStats(String type, MergeStats mergeStats) {
		this.sendGauge(type, "current", mergeStats.getCurrent());
		this.sendGauge(type, "current_docs", mergeStats.getCurrentNumDocs());
		this.sendGauge(type, "current_size_in_bytes", mergeStats.getCurrentSizeInBytes());
		this.sendCount(type, "total", mergeStats.getTotal());
		this.sendCount(type, "total_time_in_millis", mergeStats.getTotalTimeInMillis());
		this.sendCount(type, "total_docs", mergeStats.getTotalNumDocs());
		this.sendCount(type, "total_size_in_bytes", mergeStats.getTotalSizeInBytes());
	}

	private void sendRefreshStats(String type, RefreshStats refreshStats) {
		this.sendCount(type, "total", refreshStats.getTotal());
		this.sendCount(type, "total_time_in_millis", refreshStats.getTotalTimeInMillis());
	}

	private void sendFlushStats(String type, FlushStats flush) {
		this.sendCount(type, "total", flush.getTotal());
		this.sendCount(type, "total_time_in_millis", flush.getTotalTimeInMillis());
	}

	private void sendFilterCacheStats(String name, FilterCacheStats filterCache) {
		this.sendGauge(name, "memory_size_in_bytes", filterCache.getMemorySizeInBytes());
		this.sendGauge(name, "evictions", filterCache.getEvictions());
	}

	private void sendIdCacheStats(String name, IdCacheStats idCache) {
		this.sendGauge(name, "memory_size_in_bytes", idCache.getMemorySizeInBytes());
	}

	private void sendFielddataCacheStats(String name, FieldDataStats fielddataCache) {
		this.sendGauge(name, "memory_size_in_bytes", fielddataCache.getMemorySizeInBytes());
		this.sendGauge(name, "evictions", fielddataCache.getEvictions());
	}

	private void sendPercolateStats(String name, PercolateStats stats) {
		this.sendCount(name, "total", stats.getCount());
		this.sendCount(name, "time_in_millis", stats.getTimeInMillis());
		this.sendGauge(name, "current", stats.getCurrent());
		this.sendCount(name, "queries", stats.getNumQueries());

		if (stats.getMemorySizeInBytes() != -1)
			this.sendGauge(name, "memory_size_in_bytes", stats.getMemorySizeInBytes());
	}

	private void sendCompletionStats(String name, CompletionStats stats) {
		this.sendGauge(name, "size_in_bytes", stats.getSizeInBytes());
	}

	private void sendSegmentsStats(String name, SegmentsStats stats) {
		this.sendGauge(name, "count", stats.getCount());
		this.sendGauge(name, "memory_in_bytes", stats.getMemoryInBytes());
	}

	private void sendIndexingStatsStats(String type, IndexingStats.Stats stats) {
		this.sendCount(type, "index_total", stats.getIndexCount());
		this.sendCount(type, "index_time_in_millis", stats.getIndexTimeInMillis());
		this.sendGauge(type, "index_current", stats.getIndexCount());
		this.sendCount(type, "delete_total", stats.getDeleteCount());
		this.sendCount(type, "delete_time_in_millis", stats.getDeleteTimeInMillis());
		this.sendGauge(type, "delete_current", stats.getDeleteCurrent());
	}

	private void sendSearchStatsStats(String type, SearchStats.Stats stats) {
		this.sendCount(type, "query_total", stats.getQueryCount());
		this.sendCount(type, "query_time_in_millis", stats.getQueryTimeInMillis());
		this.sendGauge(type, "query_current", stats.getQueryCurrent());
		this.sendCount(type, "fetch_total", stats.getFetchCount());
		this.sendCount(type, "fetch_time_in_millis", stats.getFetchTimeInMillis());
		this.sendGauge(type, "fetch_current", stats.getFetchCurrent());
	}
}
