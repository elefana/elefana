/**
 * Copyright 2018 Viridian Software Ltd.
 */
package com.elefana.http;

import static io.restassured.RestAssured.given;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.elefana.node.NodeSettingsService;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.restassured.RestAssured;

public class HttpServerTest extends ChannelInboundHandlerAdapter {
	private static final AtomicInteger TOTAL_RESPONSES = new AtomicInteger();
	
	private final HttpServer server = new HttpServer();
	private final EventLoopGroup clientWorkerGroup = new NioEventLoopGroup();
	
	private NodeSettingsService nodeSettingsService;
	private MetricRegistry metricRegistry;

	private Counter counter;
	private Meter meter;
	private Histogram histogram;
	
	@Before
	public void setUp() {
		TOTAL_RESPONSES.set(0);
		
		nodeSettingsService = mock(NodeSettingsService.class);
		metricRegistry = mock(MetricRegistry.class);
		counter = mock(Counter.class);
		meter = mock(Meter.class);
		histogram = mock(Histogram.class);

		when(metricRegistry.counter(anyString())).thenReturn(counter);
		when(metricRegistry.meter(anyString())).thenReturn(meter);
		when(metricRegistry.histogram(anyString())).thenReturn(histogram);

		server.setNodeSettingsService(nodeSettingsService);
		server.setMetricRegistry(metricRegistry);
	}
	
	@After
	public void teardown() {
		server.stop();
		clientWorkerGroup.shutdownGracefully();
	}
	
	@Test
	public void testDefaultServer() {
		final int port = 9202;
		
		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(false);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(0);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(104857600);
		server.start("localhost", port);
		
		RestAssured.baseURI = "http://localhost:" + port;
		given()
			.request()
			.body(generateRequestBody())
		.when().
			post("/_bulk")
		.then()
			.contentType("application/json")
			.statusCode(500);
	}
	
	@Test
	public void testGzipServer() {
		final int port = 9203;
		
		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(true);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(0);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(104857600);
		server.start("localhost", port);
		
		RestAssured.baseURI = "http://localhost:" + port;
		given()
			.request()
			.header("Content-Encoding", "gzip")
			.body(generateGzipRequestBody())
		.when().
			post("/_bulk")
		.then()
			.contentType("application/json")
			.statusCode(500);
	}
	
	@Test
	@Ignore
	public void testHttpPipelinedServer() throws Exception {
		final int port = 9204;
		
		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(false);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(100);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(104857600);
		server.start("localhost", port);
		
		//Make sure regular HTTP requests still work
		RestAssured.baseURI = "http://localhost:" + port;
		given()
			.request()
			.body(generateRequestBody())
		.when().
			post("/_bulk")
		.then()
			.contentType("application/json")
			.statusCode(500);
		
		//Test pipelined request
		Bootstrap client = createHttpClient();
        ChannelFuture channelFuture = client.connect("localhost", port).sync();
        channelFuture.channel().writeAndFlush(createHttpRequest(generateRequestBody()));
        channelFuture.channel().writeAndFlush(createHttpRequest(generateRequestBody()));
        channelFuture.channel().writeAndFlush(createLastHttpRequest());
        try {
        	Thread.sleep(1000);
        } catch (Exception e) {}
        channelFuture.channel().closeFuture().sync();
        
        Assert.assertEquals(2, TOTAL_RESPONSES.get());
	}
	
	@Test
	@Ignore
	public void testGzipHttpPipelinedServer() throws Exception {
		final int port = 9205;
		
		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(true);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(100);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(104857600);
		server.start("localhost", port);
		
		//Make sure regular HTTP requests still work
		RestAssured.baseURI = "http://localhost:" + port;
		given()
			.request()
			.body(generateGzipRequestBody())
		.when().
			post("/_bulk")
		.then()
			.contentType("application/json")
			.statusCode(500);
		
		//Test pipelined request
		Bootstrap client = createHttpClient();
        ChannelFuture channelFuture = client.connect("localhost", port).sync();
        channelFuture.channel().writeAndFlush(createHttpRequest(generateGzipRequestBody()));
        channelFuture.channel().writeAndFlush(createHttpRequest(generateGzipRequestBody()));
        channelFuture.channel().writeAndFlush(createLastHttpRequest());
        try {
        	Thread.sleep(1000);
        } catch (Exception e) {}
        channelFuture.channel().closeFuture().sync();
        
        Assert.assertEquals(2, TOTAL_RESPONSES.get());
	}
	
	@Test
	public void testMaxPayloadSizeDefaultServer() {
		final int port = 9202;
		final int payloadSize = 10485760;
		
		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(false);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(0);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(payloadSize);
		server.start("localhost", port);
		
		RestAssured.baseURI = "http://localhost:" + port;
		
		String requestBody = generateRequestBody(payloadSize);
		
		given()
			.request()
			.body(requestBody)
		.when().
			post("/_bulk")
		.then()
			.contentType("application/json")
			.statusCode(500);
		
		requestBody += "TOO_LONG";
		given()
			.request()
			.body(requestBody)
		.when().
			post("/_bulk")
		.then()
			.statusCode(413);
	}

	@Test
	public void testHttpTimeout() throws InterruptedException {
		final int port = 9202;
		final int payloadSize = 10485760;
		final int httpTimeout = 2;

		when(nodeSettingsService.isHttpGzipEnabled()).thenReturn(false);
		when(nodeSettingsService.getMaxHttpPipelineEvents()).thenReturn(0);
		when(nodeSettingsService.getMaxHttpPayloadSize()).thenReturn(payloadSize);
		when(nodeSettingsService.getHttpTimeout()).thenReturn(httpTimeout);
		server.start("localhost", port);

		RestAssured.baseURI = "http://localhost:" + port;

		Bootstrap client = createHttpClient();
		ChannelFuture channelFuture = client.connect("localhost", port).sync();
		final AtomicBoolean channelClosed = new AtomicBoolean(false);
		channelFuture.channel().closeFuture().addListener(future -> {
			channelClosed.set(true);
		});
		final long startTime = System.currentTimeMillis();
		while(System.currentTimeMillis() - startTime < 1000) {
			Assert.assertEquals(false, channelClosed.get());
		}
		try {
			Thread.sleep(1000 * httpTimeout);
		} catch (Exception e) {}
		Assert.assertEquals(true, channelClosed.get());
		channelFuture.channel().closeFuture().sync();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		TOTAL_RESPONSES.incrementAndGet();
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
	}
	
	private byte [] generateGzipRequestBody() {
		try {
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

			try (GZIPOutputStream gzip = new GZIPOutputStream(outputStream)) {
			    gzip.write(generateRequestBody().getBytes());
			}
			return outputStream.toByteArray();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	private DefaultFullHttpRequest createHttpRequest(String requestBody) {
		DefaultFullHttpRequest request = createHttpRequest(requestBody.getBytes());
		request.headers().remove("Content-Encoding");
		return request;
	}
	
	private DefaultFullHttpRequest createHttpRequest(byte [] requestBody) {
		DefaultFullHttpRequest result = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
		result.headers().add("Content-Encoding", "gzip");
		result.content().writeBytes(requestBody);
		return result;
	}
	
	private DefaultHttpRequest createLastHttpRequest() {
		return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
	}
	
	private Bootstrap createHttpClient() {
		Bootstrap client = new Bootstrap();
		client.group(clientWorkerGroup);
		client.channel(NioSocketChannel.class);
		client.option(ChannelOption.SO_KEEPALIVE, true);
		client.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
            	ch.pipeline().addLast(new HttpClientCodec());
            	ch.pipeline().addLast(new HttpContentDecompressor());
                ch.pipeline().addLast(new HttpServerTest());
            }
        });
		return client;
	}
	
	private String generateRequestBody() {
		final String line1 = "{\"index\": { \"_index\" : \"test\", \"_type\" : \"type\" }}\n";
		final String line2 = "{ \"field\" : \"value\" }\n";
		StringBuilder result = new StringBuilder();
		
		for(int i = 0; i < 100; i++) {
			result.append(line1);
			result.append(line2);
		}
		return result.toString();
	}
	
	private String generateRequestBody(int expectedPayloadSize) {
		final StringBuilder result = new StringBuilder();
		StringBuilder buffer = new StringBuilder();
		
		final String requestBody = generateRequestBody();
		buffer.append(requestBody);
		
		int currentPayloadSize = 0;
		int bufferPayloadSize = 0;
		while(currentPayloadSize < expectedPayloadSize) {
			currentPayloadSize = result.toString().getBytes().length;
			bufferPayloadSize = buffer.toString().getBytes().length;

			if(currentPayloadSize + bufferPayloadSize < expectedPayloadSize) {
				result.append(buffer.toString());
				buffer.append(buffer.toString());
			} else {
				buffer = new StringBuilder();
				buffer.append(requestBody);
				bufferPayloadSize = buffer.toString().getBytes().length;
				
				if(currentPayloadSize + bufferPayloadSize > expectedPayloadSize) {
					while(currentPayloadSize < expectedPayloadSize) {
						result.append('a');
						currentPayloadSize++;
					}
				}
			}
			
			currentPayloadSize = result.toString().getBytes().length;
		}
		return result.toString();
	}
}
