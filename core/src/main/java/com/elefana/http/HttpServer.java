/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mini2Dx.natives.OsInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.elefana.ApiRouter;
import com.elefana.node.NodeInfoService;
import com.elefana.node.NodeSettingsService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;

/**
 *
 */
@Service
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
	private Meter httpRequests;
	private Histogram httpRequestSize;

	@PostConstruct
	public void postConstruct() {
		httpRequests = metricRegistry.meter(MetricRegistry.name("http", "requests"));
		httpRequestSize = metricRegistry.histogram(MetricRegistry.name("http", "requestSize"));
		
		start(nodeSettingsService.getHttpIp(), nodeSettingsService.getHttpPort());
	}

	@PreDestroy
	public void preDestroy() {
		stop();
	}
	
	public void start(String ip, int port) {
		final ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.channel(NioServerSocketChannel.class);

		if (OsInformation.isMac()) {
			//KQueue only supported by Netty on OS X >= 10.12
			try {
				final String [] macVersion = nodeInfoService.getOsStats().getOsVersion().split(".");
				switch(Integer.parseInt(macVersion[0])) {
				case 10:
					if(Integer.parseInt(macVersion[1]) > 11) {
						serverExecutor = new KQueueEventLoopGroup();
					} else {
						LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
						serverExecutor = new NioEventLoopGroup();
					}
					break;
				default:
					LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
					serverExecutor = new NioEventLoopGroup();
					break;
				}
			} catch (Exception e) {
				LOGGER.info("KQueue not supported on this Mac version - falling back to Nio");
				serverExecutor = new NioEventLoopGroup();
			}
		} else if (OsInformation.isUnix()) {
			serverExecutor = new EpollEventLoopGroup();
		} else {
			serverExecutor = new NioEventLoopGroup();
		}
		serverBootstrap.group(serverExecutor);

		serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline channelPipeline = ch.pipeline().addLast(new HttpServerCodec());
				channelPipeline = channelPipeline.addLast(new HttpServerExpectContinueHandler());

				final boolean compressionEnabled = nodeSettingsService.isGzipEnabled();
				if (compressionEnabled) {
					channelPipeline = channelPipeline.addLast("httpContentCompressor", new HttpContentCompressor(1));
				}

				channelPipeline = channelPipeline.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
				channelPipeline = channelPipeline.addLast("httpObjectAggregator",
						new HttpObjectAggregator(nodeSettingsService.getMaxHttpPayloadSize()));

				final int maxHttpPipelineEvents = nodeSettingsService.getMaxHttpPipelineEvents();
				if (maxHttpPipelineEvents > 0) {
					channelPipeline = channelPipeline.addLast("httpPipeliningHandler",
							new HttpPipeliningHandler(maxHttpPipelineEvents));
					channelPipeline = channelPipeline.addLast("httpRouter",
							new PipelinedHttpRouter(apiRouter, httpRequests, httpRequestSize));
				} else {
					channelPipeline = channelPipeline.addLast("httpRouter",
							new DefaultHttpRouter(apiRouter, httpRequests, httpRequestSize));
				}
			}
		});

		serverBootstrap.validate();
		serverBootstrap.bind(ip, port);
		LOGGER.info("Started HTTP server on " + ip + ":" + port);
	}
	
	public void stop() {
		if(serverExecutor == null) {
			return;
		}
		serverExecutor.shutdownGracefully();
	}

	public void setNodeInfoService(NodeInfoService nodeInfoService) {
		this.nodeInfoService = nodeInfoService;
	}

	public void setNodeSettingsService(NodeSettingsService nodeSettingsService) {
		this.nodeSettingsService = nodeSettingsService;
	}
}
