/*******************************************************************************
 * Copyright 2018 Viridian Software Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.elefana.api;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;

public abstract class ApiRequest<T extends ApiResponse> {
	protected final RequestExecutor requestExecutor;
	
	protected Future<T> responseFuture;

	public ApiRequest(RequestExecutor requestExecutor) {
		super();
		this.requestExecutor = requestExecutor;
	}
	
	protected abstract Callable<T> internalExecute();

	public void execute() {
		if(responseFuture != null) {
			return;
		}
		responseFuture = requestExecutor.submit(internalExecute());
	}
	
	public void cancel() {
		if(responseFuture == null) {
			return;
		}
		responseFuture.cancel(false);
	}
	
	public T get() throws ElefanaException {
		if(responseFuture == null) {
			execute();
		}
		try {
			return responseFuture.get();
		} catch (InterruptedException e) {
			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			throw new ShardFailedException(e);
		}
	}
}
