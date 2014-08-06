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

public class StatsdReporterNodeIndicesStats extends StatsdReporter {

    private final NodeIndicesStats nodeIndicesStats;

    public StatsdReporterNodeIndicesStats(NodeIndicesStats nodeIndicesStats) {
        this.nodeIndicesStats = nodeIndicesStats;
    }

    public void run()
    {
        try {
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
        catch (Exception e) {
            this.logException(e);
        }
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
}
