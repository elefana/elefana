/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

@Component
public class MetricRegistryConfiguration {
	@Autowired
	Environment environment;

	private ScheduledReporter metricsReporter;

	@PreDestroy
	public void preDestroy() {
		if (metricsReporter != null) {
			metricsReporter.stop();
		}
	}

	@Bean
	public MetricRegistry metricRegistry() {
		MetricRegistry metricRegistry = new MetricRegistry();

		switch (environment.getRequiredProperty("elefana.metrics.reporter").toLowerCase()) {
		case "graphite":
			Graphite graphite = new Graphite(
					new InetSocketAddress(environment.getRequiredProperty("elefana.metrics.graphite.host"),
							environment.getRequiredProperty("elefana.metrics.graphite.port", Integer.class)));
			metricsReporter = GraphiteReporter.forRegistry(metricRegistry)
					.prefixedWith(environment.getRequiredProperty("elefana.metrics.graphite.prefix"))
					.convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL)
					.build(graphite);
			break;
		case "console":
			metricsReporter = ConsoleReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
					.convertDurationsTo(TimeUnit.MILLISECONDS).build();
			break;
		default:
		case "noop":
			metricsReporter = null;
			break;
		}
		if (metricsReporter != null) {
			metricsReporter.start(environment.getRequiredProperty("elefana.metrics.frequency", Long.class),
					TimeUnit.MILLISECONDS);
		}
		return metricRegistry;
	}
}
