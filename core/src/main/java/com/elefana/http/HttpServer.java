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
package com.elefana.http;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import io.netty.handler.timeout.IdleStateHandler;
import org.mini2Dx.natives.OsInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.elefana.api.ApiRouter;
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;

/**
 *
 */
@Service
@DependsOn("nodeSettingsService")
public class HttpServer {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpServer.class);

	@Autowired
	private NodeSettingsService nodeSettingsService;
	@Autowired
	private NodeInfoService nodeInfoService;
	@Autowired
	private ApiRouter apiRouter;
	@Autowired
	private MetricRegistry metricRegistry;

	private EventLoopGroup serverExecutor;
	private Counter httpConnections;
	private Meter httpRequests;
	private Histogram httpRequestSize;

	@PostConstruct
	public void postConstruct() {
		if(!nodeSettingsService.isHttpEnabled()) {
			return;
		}
		
		httpConnections = metricRegistry.counter(MetricRegistry.name("http", "connections"));
		httpRequests = metricRegistry.meter(MetricRegistry.name("http", "requests"));
		httpRequestSize = metricRegistry.histogram(MetricRegistry.name("http", "requestSize"));
		
		start(nodeSettingsService.getHttpIp(), nodeSettingsService.getHttpPort());
	}

	@PreDestroy
	public void preDestroy() {
		if(!nodeSettingsService.isHttpEnabled()) {
			return;
		}
		
		stop();
	}
	
	public void start(String ip, int port) {
		final ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.option(ChannelOption.SO_BACKLOG, 1024);
		serverBootstrap.option(ChannelOption.SO_REUSEADDR, true);
		serverBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
		serverBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
		serverBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		serverBootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024, 32 * 1024));

		if (OsInformation.isMac()) {
			//KQueue only supported by Netty on OS X >= 10.12
			try {
				final String [] macVersion = nodeInfoService.getOsStats().getOsVersion().split(".");
				switch(Integer.parseInt(macVersion[0])) {
				case 10:
					if(Integer.parseInt(macVersion[1]) > 11) {
						serverBootstrap.channel(KQueueServerSocketChannel.class);
						serverExecutor = new KQueueEventLoopGroup();
					} else {
						LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
						serverBootstrap.channel(NioServerSocketChannel.class);
						serverExecutor = new NioEventLoopGroup();
					}
					break;
				default:
					LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
					serverBootstrap.channel(NioServerSocketChannel.class);
					serverExecutor = new NioEventLoopGroup();
					break;
				}
			} catch (Exception e) {
				LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
				serverBootstrap.channel(NioServerSocketChannel.class);
				serverExecutor = new NioEventLoopGroup();
			}
		} else {
			serverBootstrap.channel(NioServerSocketChannel.class);
			serverExecutor = new NioEventLoopGroup();
		}
		serverBootstrap.group(serverExecutor);

		serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				if(httpConnections != null) {
					httpConnections.inc();
				}

				ChannelPipeline channelPipeline = ch.pipeline();
				channelPipeline = ch.pipeline().addLast(new HttpServerCodec());
				channelPipeline = channelPipeline.addLast(new HttpServerExpectContinueHandler());

				final boolean compressionEnabled = nodeSettingsService.isHttpGzipEnabled();
				if (compressionEnabled) {
					channelPipeline = channelPipeline.addLast("httpContentCompressor", new HttpContentCompressor(1));
				}
				channelPipeline = channelPipeline.addLast("httpContentDecompressor", new HttpContentDecompressor());

				channelPipeline = channelPipeline.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
				channelPipeline = channelPipeline.addLast("httpObjectAggregator",
						new HttpObjectAggregator(nodeSettingsService.getMaxHttpPayloadSize()));

				final int maxHttpPipelineEvents = nodeSettingsService.getMaxHttpPipelineEvents();
				if (maxHttpPipelineEvents > 0) {
					channelPipeline = channelPipeline.addLast("httpPipeliningHandler",
							new HttpPipeliningHandler(maxHttpPipelineEvents));
					channelPipeline = channelPipeline.addLast("httpRouter",
							new PipelinedHttpRouter(apiRouter, httpConnections, httpRequests, httpRequestSize));
				} else {
					channelPipeline = channelPipeline.addLast("httpRouter",
							new DefaultHttpRouter(apiRouter, httpConnections, httpRequests, httpRequestSize));
				}
			}
		});

		serverBootstrap.validate();
		ChannelFuture channelFuture = serverBootstrap.bind(ip, port);
		try {
			channelFuture.sync();
			LOGGER.info("Started HTTP server on " + ip + ":" + port);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}
	
	public void stop() {
		if(serverExecutor == null) {
			return;
		}
		try {
			serverExecutor.shutdownGracefully().sync();
			LOGGER.info("Stopped HTTP server");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setNodeInfoService(NodeInfoService nodeInfoService) {
		this.nodeInfoService = nodeInfoService;
	}

	public void setNodeSettingsService(NodeSettingsService nodeSettingsService) {
		this.nodeSettingsService = nodeSettingsService;
	}
}
