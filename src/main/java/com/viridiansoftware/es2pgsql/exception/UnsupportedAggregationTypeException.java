/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.UNSUPPORTED_MEDIA_TYPE, reason="Unsupported aggregation type") 
public class UnsupportedAggregationTypeException extends RuntimeException {
	private static final long serialVersionUID = 4035166893370142375L;

}
