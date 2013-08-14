package org.elasticsearch.service.statsd;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.List;

public class StatsdService extends AbstractLifecycleComponent<StatsdService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private NodeService nodeService;
    private final String statsdHost;
    private final Integer statsdPort;
    private final TimeValue statsdRefreshInternal;
    private final String statsdPrefix;

    private volatile Thread statsdReporterThread;
    private volatile boolean closed;

    @Inject
    public StatsdService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                   NodeService nodeService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeService = nodeService;
        statsdRefreshInternal = settings.getAsTime("metrics.statsd.every", TimeValue.timeValueMinutes(1));
        statsdHost = settings.get("metrics.statsd.host");
        statsdPort = settings.getAsInt("metrics.statsd.port", 2003);
        statsdPrefix = settings.get("metrics.statsd.prefix", "elasticsearch" + "." + settings.get("cluster.name"));
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (statsdHost != null && statsdHost.length() > 0) {
            statsdReporterThread = EsExecutors.daemonThreadFactory(settings, "statsd_reporter").newThread(new StatsdReporterThread());
            statsdReporterThread.start();
            logger.info("Statsd reporting triggered every [{}] to host [{}:{}] with metric prefix [{}]", statsdRefreshInternal, statsdHost, statsdPort, statsdPrefix);
        } else {
            logger.error("Statsd reporting disabled, no statsd host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (closed) {
            return;
        }
        if (statsdReporterThread != null) {
            statsdReporterThread.interrupt();
        }
        closed = true;
        logger.info("Statsd reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticSearchException {}

    public class StatsdReporterThread implements Runnable {

        public void run() {
            while (!closed) {
                DiscoveryNode node = clusterService.localNode();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);

                if (isClusterStarted && node != null && node.isMasterNode()) {
                    NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
                    CommonStatsFlags commonStatsFlags = new CommonStatsFlags().clear();
                    NodeStats nodeStats = nodeService.stats(commonStatsFlags, true, true, true, true, true, true, true, true);
                    List<IndexShard> indexShards = getIndexShards(indicesService);

                    StatsdReporter statsdReporter = new StatsdReporter(statsdHost, statsdPort, statsdPrefix,
                            nodeIndicesStats, indexShards, nodeStats);
                    statsdReporter.run();
                } else {
                    if (node != null) {
                        logger.debug("[{}]/[{}] is not master node, not triggering update", node.getId(), node.getName());
                    }
                }

                try {
                    Thread.sleep(statsdRefreshInternal.millis());
                } catch (InterruptedException e1) {
                    continue;
                }
            }
        }

        private List<IndexShard> getIndexShards(IndicesService indicesService) {
            List<IndexShard> indexShards = Lists.newArrayList();
            String[] indices = indicesService.indices().toArray(new String[]{});
            for (String indexName : indices) {
                IndexService indexService = indicesService.indexServiceSafe(indexName);
                for (int shardId : indexService.shardIds()) {
                    indexShards.add(indexService.shard(shardId));
                }
            }
            return indexShards;
        }
    }
}
