# Elasticsearch StatsD Plugin

This plugin creates a little push service, which regularly updates a StatsD host with indices stats and nodes stats.
Index stats that apply across the entire cluster is only pushed from the elected master which node level stats are pushed from every node.

The data sent to the StatsD server tries to be roughly equivalent to the [Indices Stats API](http://www.elasticsearch.org/guide/reference/api/admin-indices-stats.html) and [Nodes Stats Api](http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html).


## Installation

To install a prepackaged plugin use the following command:

```
bin/plugin -install statsd -url https://github.com/Automattic/elasticsearch-statsd-plugin/releases/download/v0.3.3/elasticsearch-statsd-0.3.3.zip
```

You can also build your own by doing the following:

```
git clone http://github.com/Automattic/elasticsearch-statsd-plugin.git
cd elasticsearch-statsd-plugin
mvn package
bin/plugin -install statsd -url file:///absolute/path/to/current/dir/target/releases/elasticsearch-statsd-0.3.3.zip
```


## Configuration

Configuration is possible via these parameters:

* `metrics.statsd.host`: The statsd host to connect to (default: none)
* `metrics.statsd.port`: The port to connect to (default: 8125)
* `metrics.statsd.every`: The interval to push data (default: 1m)
* `metrics.statsd.prefix`: The metric prefix that's sent with metric names (default: elasticsearch.your_cluster_name)
* `metrics.statsd.node_name`: Override the name for node used in the stat keys (default: the ES node name)
* `metrics.statsd.report.node_indices`: If per node index sums should be reported (default: false)
* `metrics.statsd.report.indices`: If index level sums should be reported (default: true)
* `metrics.statsd.report.shards`: If shard level stats should be reported (default: false)
* `metrics.statsd.report.fs_details`: If nodes should break down the FS by device instead of total disk (default: false)

Check your elasticsearch log file for a line like this after adding the configuration parameters below to the configuration file

```
[2013-02-08 16:01:49,153][INFO ][service.statsd         ] [Sea Urchin] Statsd reporting triggered every [1m] to host [statsd.example.com:8125]
```


## Stats Key Formats

This plugin reports both node level and cluster level stats, the StatsD keys will be in the formats:

* `{PREFIX}.node.{NODE_NAME}.{STAT_KEY}`: Node level stats (CPU / JVM / etc.)
* `{PREFIX}.node.{NODE_NAME}.indices.{STAT_KEY}`: Index stats summed across the node (off by default)
* `{PREFIX}.indices.{STAT_KEY}`: Index stats summed across the entire cluster
* `{PREFIX}.index.{INDEX_NAME}.total.{STAT_KEY}`: Index stats summed per index across all shards
* `{PREFIX}.index.{INDEX_NAME}.{SHARD_ID}.{STAT_KEY}` -- Index stats per shard (off by default)


## Bugs/TODO

* Not extensively tested
* In case of a master node failover, counts are starting from 0 again (in case you are wondering about spikes)


## Credits

This is a fork of the [Swoop plugin](https://github.com/swoop-inc/elasticsearch-statsd-plugin) for multi-node clusters on ES 1.x.

Heavily inspired by the excellent [metrics library](http://metrics.codahale.com) by Coda Hale and its [GraphiteReporter add-on](http://metrics.codahale.com/manual/graphite/).


## License

See LICENSE
