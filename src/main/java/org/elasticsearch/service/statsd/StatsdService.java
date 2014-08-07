package org.elasticsearch.service.statsd;

import java.util.List;

import org.elasticsearch.ElasticsearchException;
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

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class StatsdService extends AbstractLifecycleComponent<StatsdService> {

	private final ClusterService clusterService;
	private final IndicesService indicesService;
	private final NodeService nodeService;
	private final String statsdHost;
	private final Integer statsdPort;
	private final TimeValue statsdRefreshInternal;
	private final String statsdPrefix;
	private final String statsdNodeName;
	private final Boolean statsdReportIndices;
	private final Boolean statsdReportShards;
	private final Boolean statsdReportFsDetails;
	private final StatsDClient statsdClient;

	private volatile Thread statsdReporterThread;
	private volatile boolean closed;

	@Inject
	public StatsdService(Settings settings, ClusterService clusterService, IndicesService indicesService, NodeService nodeService) {
		super(settings);
		this.clusterService = clusterService;
		this.indicesService = indicesService;
		this.nodeService = nodeService;
		this.statsdRefreshInternal = settings.getAsTime(
			"metrics.statsd.every", TimeValue.timeValueMinutes(1)
		);
		this.statsdHost = settings.get(
			"metrics.statsd.host"
		);
		this.statsdPort = settings.getAsInt(
			"metrics.statsd.port", 8125
		);
		this.statsdPrefix = settings.get(
			"metrics.statsd.prefix", "elasticsearch" + "." + settings.get("cluster.name")
		);
		this.statsdNodeName = settings.get(
			"metrics.statsd.node_name"
		);
		this.statsdReportIndices = settings.getAsBoolean(
			"metrics.statsd.report.indices", true
		);
		this.statsdReportShards = settings.getAsBoolean(
			"metrics.statsd.report.shards", false
		);
		this.statsdReportFsDetails = settings.getAsBoolean(
			"metrics.statsd.report.fs_details", false
		);
		this.statsdClient = new NonBlockingStatsDClient(this.statsdPrefix, this.statsdHost, this.statsdPort);
	}

	@Override
	protected void doStart() throws ElasticsearchException {
		if (this.statsdHost != null && this.statsdHost.length() > 0) {
			this.statsdReporterThread = EsExecutors
				.daemonThreadFactory(this.settings, "statsd_reporter")
				.newThread(new StatsdReporterThread());
			this.statsdReporterThread.start();
			this.logger.info(
				"StatsD reporting triggered every [{}] to host [{}:{}] with metric prefix [{}]",
				this.statsdRefreshInternal, this.statsdHost, this.statsdPort, this.statsdPrefix
			);
		} else {
			this.logger.error(
				"StatsD reporting disabled, no statsd host configured"
			);
		}
	}

	@Override
	protected void doStop() throws ElasticsearchException {
		if (this.closed) {
			return;
		}
		if (this.statsdReporterThread != null) {
			this.statsdReporterThread.interrupt();
		}
		this.closed = true;
		this.logger.info("StatsD reporter stopped");
	}

	@Override
	protected void doClose() throws ElasticsearchException {
	}

	public class StatsdReporterThread implements Runnable {

		@Override
		public void run() {
			while (!StatsdService.this.closed) {
				DiscoveryNode node = StatsdService.this.clusterService.localNode();
				boolean isClusterStarted = StatsdService.this.clusterService
					.lifecycleState()
					.equals(Lifecycle.State.STARTED);

				if (node != null && isClusterStarted) {
					// Report node stats -- runs for all nodes
					StatsdReporter nodeStatsReporter = new StatsdReporterNodeStats(
						StatsdService.this.nodeService.stats(
							new CommonStatsFlags().clear(), // indices
							true, // os
							true, // process
							true, // jvm
							true, // threadPool
							true, // network
							true, // fs
							true, // transport
							true, // http
							false // circuitBreaker
						),
						StatsdService.this.statsdNodeName == null ? node.getName() : StatsdService.this.statsdNodeName,
						StatsdService.this.statsdReportFsDetails
					);
					nodeStatsReporter
						.setStatsDClient(StatsdService.this.statsdClient)
						.run();

					// Master node is the only one allowed to send cluster wide sums / stats
					if (node.isMasterNode()) {
						// Report cluster wide index totals
						StatsdReporter nodeIndicesStatsReporter = new StatsdReporterNodeIndicesStats(
							StatsdService.this.indicesService.stats(
								false // includePrevious
							)
						);
						nodeIndicesStatsReporter
							.setStatsDClient(StatsdService.this.statsdClient)
							.run();

						// Maybe breakdown numbers by index or shard
						if ( StatsdService.this.statsdReportIndices || StatsdService.this.statsdReportShards ) {
							StatsdReporter indicesReporter = new StatsdReporterIndices(
								this.getIndexShards(StatsdService.this.indicesService),
								StatsdService.this.statsdReportShards
							);
							indicesReporter
								.setStatsDClient(StatsdService.this.statsdClient)
								.run();
						}
					}
				}

				try {
					Thread.sleep(StatsdService.this.statsdRefreshInternal.millis());
				} catch (InterruptedException e1) {
					continue;
				}
			}
		}

		private List<IndexShard> getIndexShards(IndicesService indicesService) {
			List<IndexShard> indexShards = Lists.newArrayList();

			String[] indices = indicesService.indices().toArray(new String[] {});
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
