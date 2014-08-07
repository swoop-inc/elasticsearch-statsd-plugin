package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.shard.service.IndexShard;

public class StatsdReporterIndices extends StatsdReporterIndexStats {

	private final List<IndexShard> indexShards;

	public StatsdReporterIndices(List<IndexShard> indexShards) {
		this.indexShards = indexShards;
	}

	public void run() {
		try {
			for (IndexShard indexShard : this.indexShards) {
				String type = this.buildMetricName("index.") + indexShard.shardId().index().name() + ".shard." + indexShard.shardId().id();
				this.sendDocsStats(type + ".docs", indexShard.docStats());
				this.sendStoreStats(type + ".store", indexShard.storeStats());
				this.sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
				this.sendGetStats(type + ".get", indexShard.getStats());
				this.sendSearchStats(type + ".search", indexShard.searchStats("_all"));
				this.sendMergeStats(type + ".merges", indexShard.mergeStats());
				this.sendRefreshStats(type + ".refresh", indexShard.refreshStats());
				this.sendFlushStats(type + ".flush", indexShard.flushStats());
				this.sendFilterCacheStats(type + ".filter_cache", indexShard.filterCacheStats());
				this.sendIdCacheStats(type + ".id_cache", indexShard.idCacheStats());
				this.sendFielddataCacheStats(type + ".fielddata", indexShard.fieldDataStats("_all"));
				this.sendCompletionStats(type + ".completion", indexShard.completionStats("_all"));
				this.sendSegmentsStats(type + ".segments", indexShard.segmentStats());
			}
		} catch (Exception e) {
			this.logException(e);
		}
	}
}
