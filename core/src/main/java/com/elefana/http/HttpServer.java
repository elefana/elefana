/**
 * Copyright 2016 Thomas Cashman
 */
package com.elefana.http;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mini2Dx.natives.OsInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.elefana.ApiRouter;

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
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;

/**
 *
 */
@Service
public class HttpServer {
	private static final int MAX_CHUCK_SIZE = 8192;

	@Autowired
	private Environment environment;
	@Autowired
	private ApiRouter apiRouter;

	private EventLoopGroup serverExecutor;

	@PostConstruct
	public void postConstruct() {
		ServerBootstrap serverBootstrap = new ServerBootstrap();
		serverBootstrap.channel(NioServerSocketChannel.class);

		if (OsInformation.isMac()) {
			serverExecutor = new KQueueEventLoopGroup();
		} else if (OsInformation.isUnix()) {
			serverExecutor = new EpollEventLoopGroup();
		} else {
			serverBootstrap.group(new NioEventLoopGroup());
		}
		serverBootstrap.group(serverExecutor);

		serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline channelPipeline = ch.pipeline().addLast(new HttpServerCodec());

				final boolean compressionEnabled = environment.getRequiredProperty("elefana.http.gzip", Boolean.class);
				if (compressionEnabled) {
					channelPipeline = channelPipeline.addLast("httpContentCompressor", new HttpContentCompressor(1));
				}

				channelPipeline = channelPipeline.addLast("httpKeepAlive", new HttpServerKeepAliveHandler());
				channelPipeline = channelPipeline.addLast("httpObjectAggregator",
						new HttpObjectAggregator(MAX_CHUCK_SIZE));

				final int maxHttpPipelineEvents = environment.getRequiredProperty("elefana.http.maxEvents",
						Integer.class);
				if (maxHttpPipelineEvents > 0) {
					channelPipeline = channelPipeline.addLast("httpPipeliningHandler",
							new HttpPipeliningHandler(maxHttpPipelineEvents));
					channelPipeline = channelPipeline.addLast("httpRouter", new PipelinedHttpRouter(apiRouter));
				} else {
					channelPipeline = channelPipeline.addLast("httpRouter", new DefaultHttpRouter(apiRouter));
				}
			}
		});

		serverBootstrap.validate();
		serverBootstrap.bind(environment.getRequiredProperty("elefana.address"),
				environment.getRequiredProperty("elefana.port", Integer.class));
	}

	@PreDestroy
	public void preDestroy() {
		serverExecutor.shutdownGracefully();
	}
}
