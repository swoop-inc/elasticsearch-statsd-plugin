package org.elasticsearch.service.statsd;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;

public class StatsdReporterIndices extends StatsdReporter {

    private final List<IndexShard> indexShards;

    public StatsdReporterIndices(List<IndexShard> indexShards) {
        this.indexShards = indexShards;
    }

    public void run()
    {
        try {
            for (IndexShard indexShard : this.indexShards) {
                String type = this.buildMetricName("indexes.") + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
                this.sendSearchStats(type + ".search", indexShard.searchStats());
                this.sendGetStats(type + ".get", indexShard.getStats());
                this.sendDocsStats(type + ".docs", indexShard.docStats());
                this.sendRefreshStats(type + ".refresh", indexShard.refreshStats());
                this.sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
                this.sendMergeStats(type + ".merge", indexShard.mergeStats());
                this.sendWarmerStats(type + ".warmer", indexShard.warmerStats());
                this.sendStoreStats(type + ".store", indexShard.storeStats());
            }
        }
        catch (Exception e) {
            this.logException(e);
        }
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

    private void sendDocsStats(String name, DocsStats docsStats)
    {
        this.sendCount(name, "count", docsStats.getCount());
        this.sendCount(name, "deleted", docsStats.getDeleted());
    }
}
