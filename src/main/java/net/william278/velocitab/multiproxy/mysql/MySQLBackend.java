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

package net.william278.velocitab.multiproxy.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.william278.velocitab.multiproxy.MultiProxyBackend;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/**
 * MySQL/MariaDB-based multi-proxy backend using HikariCP for connection pooling.
 * Uses polling to synchronize player data across proxy instances.
 */
public class MySQLBackend implements MultiProxyBackend {

    private final Logger logger;
    private final MySQLSettings settings;
    private final ExecutorService executor;
    private final Map<String, BiConsumer<String, String>> handlers;

    private HikariDataSource dataSource;
    private ScheduledExecutorService pollExecutor;
    private long lastPollTimestamp;

    public MySQLBackend(@NotNull Logger logger, @NotNull MySQLSettings settings) {
        this.logger = logger;
        this.settings = settings;
        this.executor = Executors.newFixedThreadPool(2, r -> {
            final Thread t = new Thread(r, "Velocitab MySQL Worker");
            t.setDaemon(true);
            return t;
        });
        this.handlers = new ConcurrentHashMap<>();
        this.lastPollTimestamp = System.currentTimeMillis();
    }

    @Override
    public void connect() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:mariadb://%s:%d/%s",
                settings.getHost(), settings.getPort(), settings.getDatabase()));
        config.setUsername(settings.getUsername());
        config.setPassword(settings.getPassword());
        config.setMaximumPoolSize(settings.getPoolSize());
        config.setConnectionTimeout(settings.getConnectionTimeout());
        config.setPoolName("Velocitab-HikariCP");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        this.dataSource = new HikariDataSource(config);
        createTables();

        // Start polling
        this.pollExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "Velocitab MySQL Poller");
            t.setDaemon(true);
            return t;
        });
        pollExecutor.scheduleAtFixedRate(this::pollForUpdates,
                settings.getPollInterval(), settings.getPollInterval(), TimeUnit.MILLISECONDS);

        logger.info("[Velocitab] MySQL connection pool established (pool size: {})", settings.getPoolSize());
    }

    @Override
    public void disconnect() {
        if (pollExecutor != null) {
            pollExecutor.shutdown();
        }
        executor.shutdown();
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
        logger.info("[Velocitab] MySQL connections closed");
    }

    @Override
    @NotNull
    public CompletableFuture<Void> publishAsync(@NotNull String channel, @NotNull String message) {
        return CompletableFuture.runAsync(() -> {
            final String table = settings.getTablePrefix() + "messages";
            final String sql = "INSERT INTO " + table + " (channel, message, created_at) VALUES (?, ?, ?)";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, channel);
                stmt.setString(2, message);
                stmt.setLong(3, System.currentTimeMillis());
                stmt.executeUpdate();
            } catch (SQLException e) {
                logger.error("[Velocitab] Failed to publish message to MySQL channel {}", channel, e);
            }
        }, executor);
    }

    @Override
    public void subscribe(@NotNull String channel, @NotNull BiConsumer<String, String> handler) {
        handlers.put(channel, handler);
    }

    private void pollForUpdates() {
        final String table = settings.getTablePrefix() + "messages";
        final String sql = "SELECT id, channel, message FROM " + table + " WHERE created_at > ? ORDER BY id ASC";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, lastPollTimestamp);
            lastPollTimestamp = System.currentTimeMillis();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    final String channel = rs.getString("channel");
                    final String message = rs.getString("message");
                    final BiConsumer<String, String> handler = handlers.get(channel);
                    if (handler != null) {
                        try {
                            handler.accept(channel, message);
                        } catch (Exception e) {
                            logger.error("[Velocitab] Error handling polled message on {}", channel, e);
                        }
                    }
                }
            }

            // Clean old messages (older than 60 seconds)
            final String cleanSql = "DELETE FROM " + table + " WHERE created_at < ?";
            try (PreparedStatement cleanStmt = conn.prepareStatement(cleanSql)) {
                cleanStmt.setLong(1, System.currentTimeMillis() - 60000);
                cleanStmt.executeUpdate();
            }
        } catch (SQLException e) {
            logger.error("[Velocitab] Failed to poll for MySQL messages", e);
        }
    }

    private void createTables() {
        final String messagesTable = settings.getTablePrefix() + "messages";
        final String proxiesTable = settings.getTablePrefix() + "proxies";

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS " + messagesTable + " (" +
                            "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                            "channel VARCHAR(128) NOT NULL, " +
                            "message MEDIUMTEXT NOT NULL, " +
                            "created_at BIGINT NOT NULL, " +
                            "INDEX idx_created (created_at)" +
                            ")")) {
                stmt.executeUpdate();
            }

            try (PreparedStatement stmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS " + proxiesTable + " (" +
                            "proxy_id VARCHAR(8) PRIMARY KEY, " +
                            "last_heartbeat BIGINT NOT NULL" +
                            ")")) {
                stmt.executeUpdate();
            }

            logger.info("[Velocitab] MySQL tables created/verified");
        } catch (SQLException e) {
            logger.error("[Velocitab] Failed to create MySQL tables", e);
        }
    }
}
