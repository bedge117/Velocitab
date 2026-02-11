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

package net.william278.velocitab.multiproxy;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Interface for multi-proxy communication backends.
 * Implementations must be fully async and thread-safe.
 */
public interface MultiProxyBackend {

    /**
     * Establish connections to the backend.
     */
    void connect();

    /**
     * Gracefully disconnect from the backend.
     */
    void disconnect();

    /**
     * Publish a message to a channel asynchronously.
     *
     * @param channel the channel name (without prefix)
     * @param message the message payload
     * @return a future that completes when the message is acknowledged
     */
    @NotNull
    CompletableFuture<Void> publishAsync(@NotNull String channel, @NotNull String message);

    /**
     * Subscribe to a channel with a message handler.
     * The handler receives the channel name and the message payload.
     * The handler MUST NOT block — dispatch heavy work to another thread.
     *
     * @param channel the channel name (without prefix)
     * @param handler the message handler
     */
    void subscribe(@NotNull String channel, @NotNull BiConsumer<String, String> handler);
}
