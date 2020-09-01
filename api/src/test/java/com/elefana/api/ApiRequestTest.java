/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.api;

import com.elefana.api.exception.ElefanaException;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ApiRequestTest {
	final DummyApiRequestExecutor requestExecutor = new DummyApiRequestExecutor();

	@After
	public void teardown() {
		requestExecutor.shutdown();
	}

	@Test
	public void testExecute() {
		final EmbeddedChannel channel = new EmbeddedChannel();
		final DummyHandler handler = new DummyHandler();
		channel.pipeline().addLast(handler);
		final ChannelHandlerContext context = channel.pipeline().context(handler);

		final DummyApiRequest request = new DummyApiRequest(requestExecutor, context);
		final AtomicBoolean result = new AtomicBoolean();
		request.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<? super Void>>() {
			@Override
			public void operationComplete(io.netty.util.concurrent.Future<? super Void> future) throws Exception {
				result.set(future.isSuccess());
			}
		});
		request.execute();
		try {
			final AckResponse response = request.get();
			Assert.assertNotNull(response);
			Assert.assertTrue(result.get());
		} catch (ElefanaException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void testExecuteNullContext() {
		final DummyApiRequest request = new DummyApiRequest(requestExecutor, null);
		request.execute();
		try {
			final AckResponse response = request.get();
			Assert.assertNotNull(response);
		} catch (ElefanaException e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private class DummyApiRequest extends ApiRequest<AckResponse> {

		public DummyApiRequest(RequestExecutor requestExecutor, ChannelHandlerContext context) {
			super(requestExecutor, context);
		}

		@Override
		protected Callable<AckResponse> internalExecute() {
			return new Callable<AckResponse>() {
				@Override
				public AckResponse call() throws Exception {
					return new AckResponse();
				}
			};
		}
	}

	private class DummyApiRequestExecutor implements RequestExecutor {
		private final ExecutorService executorService = Executors.newFixedThreadPool(1);

		public void shutdown() {
			executorService.shutdownNow();
		}

		@Override
		public <T> Future<T> submit(Callable<T> request) {
			return executorService.submit(request);
		}
	}

	private class DummyHandler extends ChannelHandlerAdapter {

	}
}
