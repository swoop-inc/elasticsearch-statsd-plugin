package org.elasticsearch.plugin.statsd;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.service.statsd.StatsdService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class StatsdPlugin extends AbstractPlugin {

	protected final Logger	log	= LoggerFactory.getLogger(this.getClass());
	
	public StatsdPlugin()
	{
		log.info("Instantiating StatsdPlugin");
	}
	
    public String name() {
        return "statsd";
    }

    public String description() {
        return "Statsd Monitoring Plugin";
    }

    @SuppressWarnings("rawtypes")
    @Override public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(StatsdService.class);
        return services;
    }

}
