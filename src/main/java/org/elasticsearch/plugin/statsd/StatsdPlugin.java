package org.elasticsearch.plugin.statsd;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.service.statsd.StatsdService;

import java.util.Collection;

public class StatsdPlugin extends AbstractPlugin {

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
