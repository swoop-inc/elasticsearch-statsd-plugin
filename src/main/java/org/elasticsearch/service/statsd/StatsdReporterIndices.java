package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;

public class StatsdReporterIndices extends StatsdReporterIndexStats {

	private final List<IndexShard> indexShards;
	private final Boolean reportIndices;
	private final Boolean reportShards;
	private String indexName;
	private CommonStats indexStats;
	private CommonStats indexTotalStats;

	public StatsdReporterIndices(List<IndexShard> indexShards, Boolean reportIndices, Boolean reportShards) {
		this.indexShards = indexShards;
		this.reportIndices = reportIndices;
		this.reportShards = reportShards;
	}

	public void run() {
		try {
			this.indexTotalStats = new CommonStats(CommonStatsFlags.ALL);

			for (IndexShard indexShard : this.indexShards) {
				// Create common stats for shard
				CommonStats shardStats = new CommonStats(indexShard, CommonStatsFlags.ALL);

				if (this.reportShards) {
					this.sendCommonStats(
						this.buildMetricName("index." + this.indexName + "." + indexShard.shardId().id()),
						shardStats
					);
				}

				if (this.reportIndices) {
					this.maybeResetSums(indexShard.shardId().index().name());
					this.indexStats.add(shardStats);
				}

				this.indexTotalStats.add(shardStats);
			}

			// Send last index group... maybe
			if (this.reportIndices) {
				this.maybeResetSums("");
			}

			// Send index totals
			this.sendCommonStats(
				this.buildMetricName("indices"),
				this.indexTotalStats
			);
		} catch (Exception e) {
			this.logException(e);
		}
	}

	private void maybeResetSums(String newIndexName) {
		if (this.indexName == newIndexName) {
			return; // Same index do nothing
		}

		// Index name changed to some other value, send the last sum
		if (this.indexName != null) {
			this.sendCommonStats(
				this.buildMetricName("index." + this.indexName + ".total"),
				this.indexStats
			);
		}

		// Set new index name and reset to empty common stats
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
