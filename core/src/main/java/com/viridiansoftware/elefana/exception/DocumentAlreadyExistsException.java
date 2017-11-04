/**
 * Copyright 2017 Viridian Software Ltd.
 */
package com.viridiansoftware.elefana.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value=HttpStatus.CONFLICT, reason="Document not found") 
public class DocumentAlreadyExistsException extends RuntimeException {
	private static final long serialVersionUID = -6892951993135542009L;

}
