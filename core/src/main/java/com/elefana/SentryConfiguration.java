/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana;

import io.sentry.Sentry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

@Configuration
public class SentryConfiguration {
	@Autowired
	Environment environment;

	@PostConstruct
	public void postConstruct() {
		final String sentryDsn = environment.getProperty("elefana.sentry.dsn", "");
		if(!sentryDsn.isEmpty()) {
			Sentry.init(sentryDsn);
		}
	}
}
