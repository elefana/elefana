/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.INTERNAL_SERVER_ERROR, reason="Shard failed") 
public class ShardFailedException extends RuntimeException {
	private static final long serialVersionUID = -1696903796723939014L;

}
