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

import com.elefana.api.exception.ElefanaException;
import com.elefana.api.exception.ShardFailedException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ApiRequest<T extends ApiResponse> {
	@JsonIgnore
	protected final RequestExecutor requestExecutor;
	@JsonIgnore
	protected final boolean streamingResponse;
	@JsonIgnore
	protected final ChannelHandlerContext context;
	@JsonIgnore
	protected final ChannelPromise channelPromise;
	@JsonIgnore
	protected final AtomicBoolean executionStarted = new AtomicBoolean(false);

	@JsonIgnore
	protected Future<T> backingFuture;

	public ApiRequest(RequestExecutor requestExecutor, ChannelHandlerContext context) {
		this(requestExecutor, context, false);
	}

	public ApiRequest(RequestExecutor requestExecutor, ChannelHandlerContext context, boolean streamingResponse) {
		super();
		this.requestExecutor = requestExecutor;
		this.streamingResponse = streamingResponse;

		if(context == null) {
			this.context = null;
			channelPromise = null;
			return;
		}
		this.context = context;
		channelPromise = context.newPromise();
	}
	
	protected abstract Callable<T> internalExecute();

	public void execute() {
		if(executionStarted.getAndSet(true)) {
			return;
		}
		if(channelPromise == null) {
			backingFuture = requestExecutor.submit(internalExecute());
		} else {
			backingFuture = requestExecutor.submit(internalExecute(), channelPromise);
		}
	}
	
	public void cancel() {
		if(!channelPromise.cancel(true)) {
			return;
		}
		backingFuture.cancel(true);
	}

	public T get() throws ElefanaException {
		try {
			if(backingFuture == null) {
				execute();
			}
			return backingFuture.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new ShardFailedException(e);
		} catch (ExecutionException e) {
			if(e.getCause() instanceof ElefanaException) {
				throw (ElefanaException) e.getCause();
			}
			e.printStackTrace();
			throw new ShardFailedException(e);
		} catch (Exception e) {
			if(e.getCause() instanceof ElefanaException) {
				throw (ElefanaException) e.getCause();
			}
			e.printStackTrace();
			throw new ShardFailedException(e);
		}
	}
	
	public boolean isDone() {
		if(!executionStarted.get()) {
			return false;
		}
		if(channelPromise == null) {
			return backingFuture.isDone();
		}
		return channelPromise.isDone();
	}

	public boolean isStreamingResponse() {
		return streamingResponse;
	}

	public Channel channel() {
		if(context == null) {
			return null;
		}
		return context.channel();
	}

	public ChannelPromise setSuccess(Void result) {
		if(context == null) {
			return null;
		}
		return channelPromise.setSuccess(result);
	}

	public ChannelPromise setSuccess() {
		if(context == null) {
			return null;
		}
		return channelPromise.setSuccess();
	}

	public boolean trySuccess() {
		if(context == null) {
			return false;
		}
		return channelPromise.trySuccess();
	}

	public ChannelPromise setFailure(Throwable cause) {
		if(context == null) {
			return null;
		}
		return channelPromise.setFailure(cause);
	}

	public ChannelPromise addListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Void>> listener) {
		if(context == null) {
			return null;
		}
		return channelPromise.addListener(listener);
	}

	public ChannelPromise addListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Void>>... listeners) {
		if(context == null) {
			return null;
		}
		return channelPromise.addListeners(listeners);
	}

	public ChannelPromise removeListener(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Void>> listener) {
		if(context == null) {
			return null;
		}
		return channelPromise.removeListener(listener);
	}

	public ChannelPromise removeListeners(GenericFutureListener<? extends io.netty.util.concurrent.Future<? super Void>>... listeners) {
		if(context == null) {
			return null;
		}
		return channelPromise.removeListeners(listeners);
	}

	public ChannelPromise sync() throws InterruptedException {
		if(context == null) {
			return null;
		}
		return channelPromise.sync();
	}

	public ChannelPromise syncUninterruptibly() {
		if(context == null) {
			return null;
		}
		return channelPromise.syncUninterruptibly();
	}

	public ChannelPromise await() throws InterruptedException {
		if(context == null) {
			return null;
		}
		return channelPromise.await();
	}

	public ChannelPromise awaitUninterruptibly() {
		if(context == null) {
			return null;
		}
		return channelPromise.awaitUninterruptibly();
	}

	public ChannelPromise unvoid() {
		if(context == null) {
			return null;
		}
		return channelPromise.unvoid();
	}
}
