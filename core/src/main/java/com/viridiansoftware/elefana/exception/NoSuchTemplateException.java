/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.NOT_FOUND, reason="Template not found") 
public class NoSuchTemplateException extends RuntimeException {
	private static final long serialVersionUID = -7573981372413154677L;

}
