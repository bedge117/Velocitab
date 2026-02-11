/*
 * This file is part of Velocitab, licensed under the Apache License 2.0.
 *
 *  Copyright (c) William278 <will27528@gmail.com>
 *  Copyright (c) contributors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package net.william278.velocitab.multiproxy.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.event.connection.ConnectionActivatedEvent;
import io.lettuce.core.event.connection.ConnectionDeactivatedEvent;
import io.lettuce.core.event.connection.ReconnectFailedEvent;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import net.william278.velocitab.multiproxy.MultiProxyBackend;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Redis-based multi-proxy backend using Lettuce for async, non-blocking pub/sub.
 * Uses two connections: one for subscribing (dedicated), one for publishing (shared, thread-safe).
 */
public class RedisBackend implements MultiProxyBackend {

    private final Logger logger;
    private final RedisSettings settings;
    private final Map<String, BiConsumer<String, String>> handlers;

    private RedisClient client;
    private StatefulRedisConnection<String, String> publishConnection;
    private StatefulRedisPubSubConnection<String, String> subscribeConnection;

    public RedisBackend(@NotNull Logger logger, @NotNull RedisSettings settings) {
        this.logger = logger;
        this.settings = settings;
        this.handlers = new ConcurrentHashMap<>();
    }

    @Override
    public void connect() {
        final RedisURI uri = buildUri();
        this.client = RedisClient.create(uri);
        this.client.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .pingBeforeActivateConnection(true)
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(settings.getCommandTimeoutMs())))
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.ACCEPT_COMMANDS)
                .build()
        );

        // Monitor connection state
        client.getResources().eventBus().get().subscribe(event -> {
            if (event instanceof ConnectionActivatedEvent) {
                logger.info("[Velocitab] Redis connection activated");
            } else if (event instanceof ConnectionDeactivatedEvent) {
                logger.warn("[Velocitab] Redis connection lost — auto-reconnect will retry");
            } else if (event instanceof ReconnectFailedEvent rfe) {
                logger.error("[Velocitab] Redis reconnect attempt {} failed: {}",
                        rfe.getAttempt(), rfe.getCause().getMessage());
            }
        });

        // Open connections
        this.publishConnection = client.connect();
        this.subscribeConnection = client.connectPubSub();

        // Register the global message dispatcher
        subscribeConnection.addListener(new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                dispatchMessage(channel, message);
            }
        });

        logger.info("[Velocitab] Redis connections established (prefix: {})", settings.getChannelPrefix());
    }

    @Override
    public void disconnect() {
        try {
            if (subscribeConnection != null && subscribeConnection.isOpen()) {
                subscribeConnection.async().unsubscribe().await(2, TimeUnit.SECONDS);
                subscribeConnection.close();
            }
            if (publishConnection != null && publishConnection.isOpen()) {
                publishConnection.close();
            }
            if (client != null) {
                client.shutdown(Duration.ZERO, Duration.ofSeconds(2));
            }
            logger.info("[Velocitab] Redis connections closed");
        } catch (Exception e) {
            logger.error("[Velocitab] Error during Redis shutdown", e);
            if (client != null) {
                client.shutdown(Duration.ZERO, Duration.ofMillis(100));
            }
        }
    }

    @Override
    @NotNull
    public CompletableFuture<Void> publishAsync(@NotNull String channel, @NotNull String message) {
        final String fullChannel = settings.getChannelPrefix() + ":" + channel;
        final RedisAsyncCommands<String, String> async = publishConnection.async();
        return async.publish(fullChannel, message)
                .toCompletableFuture()
                .thenAccept(count -> {})
                .exceptionally(throwable -> {
                    logger.error("[Velocitab] Failed to publish to {}: {}", fullChannel, throwable.getMessage());
                    return null;
                });
    }

    @Override
    public void subscribe(@NotNull String channel, @NotNull BiConsumer<String, String> handler) {
        final String fullChannel = settings.getChannelPrefix() + ":" + channel;
        handlers.put(fullChannel, handler);
        final RedisPubSubAsyncCommands<String, String> async = subscribeConnection.async();
        async.subscribe(fullChannel);
    }

    private void dispatchMessage(@NotNull String channel, @NotNull String message) {
        final BiConsumer<String, String> handler = handlers.get(channel);
        if (handler != null) {
            try {
                handler.accept(channel, message);
            } catch (Exception e) {
                logger.error("[Velocitab] Error handling message on {}", channel, e);
            }
        }
    }

    @NotNull
    private RedisURI buildUri() {
        if (settings.isSentinel()) {
            final var nodes = settings.getSentinelNodes();
            if (nodes.isEmpty()) {
                throw new IllegalStateException("Redis Sentinel is enabled but no sentinel nodes are configured");
            }

            final String[] firstNode = nodes.get(0).split(":");
            final RedisURI.Builder builder = RedisURI.Builder.sentinel(
                    firstNode[0],
                    firstNode.length > 1 ? Integer.parseInt(firstNode[1]) : 26379,
                    settings.getSentinelMaster()
            );

            for (int i = 1; i < nodes.size(); i++) {
                final String[] node = nodes.get(i).split(":");
                builder.withSentinel(node[0], node.length > 1 ? Integer.parseInt(node[1]) : 26379);
            }

            if (!settings.getPassword().isEmpty()) {
                builder.withPassword(settings.getPassword().toCharArray());
            }
            builder.withTimeout(Duration.ofMillis(settings.getConnectionTimeoutMs()));
            return builder.build();
        } else {
            final RedisURI.Builder builder = RedisURI.Builder
                    .redis(settings.getHost(), settings.getPort())
                    .withDatabase(settings.getDatabase())
                    .withTimeout(Duration.ofMillis(settings.getConnectionTimeoutMs()));

            if (settings.isUseSsl()) {
                builder.withSsl(true);
            }
            if (!settings.getPassword().isEmpty()) {
                builder.withPassword(settings.getPassword().toCharArray());
            }
            return builder.build();
        }
    }
}
