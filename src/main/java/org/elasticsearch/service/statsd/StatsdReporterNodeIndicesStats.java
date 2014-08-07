package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.indices.NodeIndicesStats;

public class StatsdReporterNodeIndicesStats extends StatsdReporterIndexStats {

	private final NodeIndicesStats nodeIndicesStats;

	public StatsdReporterNodeIndicesStats(NodeIndicesStats nodeIndicesStats) {
		this.nodeIndicesStats = nodeIndicesStats;
	}

	public void run() {
		try {
			String type = this.buildMetricName("indices");
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
		} catch (Exception e) {
			this.logException(e);
		}
	}
}
