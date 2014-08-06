package org.elasticsearch.module.statsd.test;

import static org.elasticsearch.common.base.Predicates.containsPattern;
import static org.elasticsearch.module.statsd.test.NodeTestHelper.createNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.collect.Iterables;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StatsdPluginIntegrationTest
{

	public static final int		STATSD_SERVER_PORT	= 12345;

	private StatsdMockServer	statsdMockServer;

	private String				clusterName			= RandomStringGenerator.randomAlphabetic(10);
	private String				index				= RandomStringGenerator.randomAlphabetic(6).toLowerCase();
	private String				type				= RandomStringGenerator.randomAlphabetic(6).toLowerCase();
	private Node				node;

	@Before
	public void startStatsdMockServerAndNode() throws Exception
	{
		statsdMockServer = new StatsdMockServer(STATSD_SERVER_PORT);
		statsdMockServer.start();
		node = createNode(clusterName, 1, STATSD_SERVER_PORT, "1s");
	}

	@After
	public void stopStatsdServer() throws Exception
	{
		statsdMockServer.close();
		if (!node.isClosed()) {
			node.close();
		}
	}

	@Test
	public void testThatIndexingResultsInMonitoring() throws Exception
	{
		IndexResponse indexResponse = indexElement(node, index, type, "value");
		assertThat(indexResponse.getId(), is(notNullValue()));

		Thread.sleep(2000);

		ensureValidKeyNames();
		assertStatsdMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount:1|c");
		assertStatsdMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing." + type + ".indexCount:1|c");
		assertStatsdMetricIsContained("elasticsearch." + clusterName + ".node.jvm.threads.peak_count:");
	}

	@Test
	public void masterFailOverShouldWork() throws Exception
	{
		String clusterName = RandomStringGenerator.randomAlphabetic(10);
		IndexResponse indexResponse = indexElement(node, index, type, "value");
		assertThat(indexResponse.getId(), is(notNullValue()));

		Node origNode = node;
		node = createNode(clusterName, 1, STATSD_SERVER_PORT, "1s");
		statsdMockServer.content.clear();
		origNode.stop();
		indexResponse = indexElement(node, index, type, "value");
		assertThat(indexResponse.getId(), is(notNullValue()));

		// wait for master fail over and writing to graph reporter
		Thread.sleep(2000);
		assertStatsdMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount:1|c");
	}

	// the stupid hamcrest matchers have compile erros depending whether they run on java6 or java7, so I rolled my own version
	// yes, I know this sucks... I want power asserts, as usual
	private void assertStatsdMetricIsContained(final String id)
	{
		assertThat(Iterables.any(statsdMockServer.content, containsPattern(id)), is(true));
	}

	// Make sure no elements with a chars [] are included
	private void ensureValidKeyNames()
	{
		assertThat(Iterables.any(statsdMockServer.content, containsPattern("\\.\\.")), is(false));
		assertThat(Iterables.any(statsdMockServer.content, containsPattern("\\[")), is(false));
		assertThat(Iterables.any(statsdMockServer.content, containsPattern("\\]")), is(false));
		assertThat(Iterables.any(statsdMockServer.content, containsPattern("\\(")), is(false));
		assertThat(Iterables.any(statsdMockServer.content, containsPattern("\\)")), is(false));
	}

	private IndexResponse indexElement(Node node, String index, String type, String fieldValue)
	{
		return node.client().prepareIndex(index, type).setSource("field", fieldValue).execute().actionGet();
	}
}
