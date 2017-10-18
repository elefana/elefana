/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.es2pgsql.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.INTERNAL_SERVER_ERROR, reason="No mapping for queried field") 
public class NoSuchMappingException extends RuntimeException {
	private static final long serialVersionUID = 6356913353166607360L;

	public NoSuchMappingException(String fieldName) {
		super("No mapping for field '" + fieldName + "' was found");
	}
}
