package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;

public class StatsdReporterIndices extends StatsdReporterIndexStats {

	private final List<IndexShard> indexShards;
	private final Boolean reportShards;
	private String indexName;
	private CommonStats indexStats;

	public StatsdReporterIndices(List<IndexShard> indexShards, Boolean reportShards) {
		this.indexShards = indexShards;
		this.reportShards = reportShards;
	}

	public void run() {
		try {
			for (IndexShard indexShard : this.indexShards) {
				this.maybeResetSums(indexShard.shardId().index().name());

				// Create common stats for shard
				CommonStats shardStats = new CommonStats(indexShard, CommonStatsFlags.ALL);

				if (this.reportShards) {
					this.sendCommonStats(
						this.buildMetricName("index." + this.indexName + ".shard." + indexShard.shardId().id()),
						shardStats
					);
				}

				// Add to current index totals
				this.indexStats.add(shardStats);
			}
			// Send last index group
			this.maybeResetSums("");
		} catch (Exception e) {
			this.logException(e);
		}
	}

	private void maybeResetSums(String newIndexName) {
		if (this.indexName == newIndexName) {
			return; // Same index do nothing
		}

		if (this.indexName != null) {
			this.sendCommonStats(
				this.buildMetricName("index." + this.indexName + ".total"),
				this.indexStats
			);
		}

		this.indexName = newIndexName;
		this.indexStats = new CommonStats(CommonStatsFlags.ALL);
	}

	private void sendCommonStats(String prefix, CommonStats stats) {
		this.sendDocsStats(prefix + ".docs", stats.getDocs());
		this.sendStoreStats(prefix + ".store", stats.getStore());
		this.sendIndexingStats(prefix + ".indexing", stats.getIndexing());
		this.sendGetStats(prefix + ".get", stats.getGet());
		this.sendSearchStats(prefix + ".search", stats.getSearch());
		this.sendMergeStats(prefix + ".merges", stats.getMerge());
		this.sendRefreshStats(prefix + ".refresh", stats.getRefresh());
		this.sendFlushStats(prefix + ".flush", stats.getFlush());
		//TODO: getWarmer
		this.sendFilterCacheStats(prefix + ".filter_cache", stats.getFilterCache());
		this.sendIdCacheStats(prefix + ".id_cache", stats.getIdCache());
		this.sendFielddataCacheStats(prefix + ".fielddata", stats.getFieldData());
		//TODO: getPercolate
		this.sendCompletionStats(prefix + ".completion", stats.getCompletion());
		this.sendSegmentsStats(prefix + ".segments", stats.getSegments());
		//TODO: getTranslog
		//TODO: getSuggest
	}
}
