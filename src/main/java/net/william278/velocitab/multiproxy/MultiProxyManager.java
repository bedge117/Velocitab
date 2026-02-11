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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.Group;
import net.william278.velocitab.multiproxy.mysql.MySQLBackend;
import net.william278.velocitab.multiproxy.redis.RedisBackend;
import net.william278.velocitab.player.TabPlayer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates multi-proxy player synchronization via Redis or MySQL.
 */
public class MultiProxyManager {

    private static final String CHANNEL_JOIN = "join";
    private static final String CHANNEL_QUIT = "quit";
    private static final String CHANNEL_UPDATE = "update";
    private static final String CHANNEL_HEARTBEAT = "heartbeat";
    private static final String CHANNEL_SHUTDOWN = "shutdown";

    private static final Gson GSON = new GsonBuilder().create();

    private final Velocitab plugin;
    @Getter
    private final String proxyId;
    private final MultiProxyBackend backend;
    private final MultiProxySettings settings;

    // Track which proxy each remote player belongs to
    private final Map<UUID, String> remotePlayerProxyMap;
    // Track last heartbeat time per proxy
    private final Map<String, Long> proxyHeartbeats;

    private ScheduledExecutorService heartbeatExecutor;

    public MultiProxyManager(@NotNull Velocitab plugin) {
        this.plugin = plugin;
        this.settings = plugin.getSettings().getMultiProxy();

        // Generate or use configured proxy ID
        final String configId = settings.getProxyId();
        this.proxyId = "auto".equalsIgnoreCase(configId)
                ? UUID.randomUUID().toString().substring(0, 8)
                : configId;

        // Create backend based on config
        this.backend = switch (settings.getBackendType()) {
            case REDIS -> new RedisBackend(plugin.getLogger(), settings.getRedis());
            case MYSQL -> new MySQLBackend(plugin.getLogger(), settings.getMysql());
        };

        this.remotePlayerProxyMap = new ConcurrentHashMap<>();
        this.proxyHeartbeats = new ConcurrentHashMap<>();
    }

    /**
     * Enable multi-proxy synchronization.
     */
    public void enable() {
        backend.connect();

        // Subscribe to all channels
        backend.subscribe(CHANNEL_JOIN, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteJoin(message)));
        backend.subscribe(CHANNEL_QUIT, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteQuit(message)));
        backend.subscribe(CHANNEL_UPDATE, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteUpdate(message)));
        backend.subscribe(CHANNEL_HEARTBEAT, (channel, message) ->
                handleHeartbeat(message));
        backend.subscribe(CHANNEL_SHUTDOWN, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteShutdown(message)));

        // Start heartbeat and update tasks
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "Velocitab MultiProxy Heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat,
                settings.getHeartbeatInterval(), settings.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

        heartbeatExecutor.scheduleAtFixedRate(this::cleanStaleProxies,
                settings.getHeartbeatTimeout(), settings.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

        heartbeatExecutor.scheduleAtFixedRate(this::publishAllPlayerUpdates,
                settings.getUpdateInterval(), settings.getUpdateInterval(), TimeUnit.MILLISECONDS);

        plugin.getLogger().info("[Velocitab] Multi-proxy enabled (id: {}, backend: {})",
                proxyId, settings.getBackendType());
    }

    /**
     * Disable multi-proxy synchronization.
     */
    public void disable() {
        // Publish shutdown so other proxies clean up immediately
        final JsonObject shutdown = new JsonObject();
        shutdown.addProperty("proxyId", proxyId);
        backend.publishAsync(CHANNEL_SHUTDOWN, GSON.toJson(shutdown));

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        backend.disconnect();
        plugin.getLogger().info("[Velocitab] Multi-proxy disabled");
    }

    /**
     * Publish a player join event to other proxies.
     */
    public void publishPlayerJoin(@NotNull TabPlayer tabPlayer) {
        final PlayerSnapshot snapshot = PlayerSnapshot.capture(tabPlayer, proxyId);
        backend.publishAsync(CHANNEL_JOIN, snapshot.toJson());
    }

    /**
     * Publish a player quit event to other proxies.
     */
    public void publishPlayerQuit(@NotNull UUID uuid) {
        final JsonObject quit = new JsonObject();
        quit.addProperty("proxyId", proxyId);
        quit.addProperty("uuid", uuid.toString());
        backend.publishAsync(CHANNEL_QUIT, GSON.toJson(quit));
    }

    /**
     * Publish a player update event to other proxies.
     */
    public void publishPlayerUpdate(@NotNull TabPlayer tabPlayer) {
        final PlayerSnapshot snapshot = PlayerSnapshot.capture(tabPlayer, proxyId);
        backend.publishAsync(CHANNEL_UPDATE, snapshot.toJson());
    }

    private void handleRemoteJoin(@NotNull String message) {
        final PlayerSnapshot snapshot = PlayerSnapshot.fromJson(message);
        if (proxyId.equals(snapshot.getProxyId())) {
            return; // Ignore own messages
        }

        final UUID uuid = snapshot.getUuid();

        // Find or default the group
        final Group group = resolveGroup(snapshot.getGroupName());
        if (group == null) {
            return;
        }

        // Create or update remote player
        final TabPlayer existing = plugin.getTabList().getPlayers().get(uuid);
        if (existing instanceof RemoteTabPlayer remote) {
            remote.updateFromSnapshot(snapshot);
            remote.setGroup(group);
        } else {
            final RemoteTabPlayer remote = new RemoteTabPlayer(plugin, snapshot, group);
            plugin.getTabList().getPlayers().put(uuid, remote);
            remotePlayerProxyMap.put(uuid, snapshot.getProxyId());
        }

        // Add to local viewers' tab lists
        addRemoteToLocalViewers(plugin.getTabList().getPlayers().get(uuid));
    }

    private void handleRemoteQuit(@NotNull String message) {
        final JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        final String sourceProxyId = json.get("proxyId").getAsString();
        if (proxyId.equals(sourceProxyId)) {
            return;
        }

        final UUID uuid = UUID.fromString(json.get("uuid").getAsString());
        removeRemotePlayer(uuid);
    }

    private void handleRemoteUpdate(@NotNull String message) {
        final PlayerSnapshot snapshot = PlayerSnapshot.fromJson(message);
        if (proxyId.equals(snapshot.getProxyId())) {
            return;
        }

        final UUID uuid = snapshot.getUuid();
        final TabPlayer existing = plugin.getTabList().getPlayers().get(uuid);

        if (existing instanceof RemoteTabPlayer remote) {
            // Check if group changed
            final Group newGroup = resolveGroup(snapshot.getGroupName());
            if (newGroup == null) {
                return;
            }

            final boolean groupChanged = !remote.getGroup().equals(newGroup);
            remote.updateFromSnapshot(snapshot);

            if (groupChanged) {
                // Remove from old group viewers, add to new
                removeRemoteFromLocalViewers(uuid);
                remote.setGroup(newGroup);
                addRemoteToLocalViewers(remote);
            } else {
                // Update display name for local viewers
                updateRemoteForLocalViewers(remote);
            }
        } else {
            // Not tracked yet — treat as join
            handleRemoteJoin(message);
        }
    }

    private void handleHeartbeat(@NotNull String message) {
        final JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        final String sourceProxyId = json.get("proxyId").getAsString();
        if (proxyId.equals(sourceProxyId)) {
            return;
        }

        proxyHeartbeats.put(sourceProxyId, System.currentTimeMillis());
    }

    private void handleRemoteShutdown(@NotNull String message) {
        final JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        final String sourceProxyId = json.get("proxyId").getAsString();
        if (proxyId.equals(sourceProxyId)) {
            return;
        }

        plugin.getLogger().info("[Velocitab] Proxy {} shut down, removing its players", sourceProxyId);
        removeAllPlayersFromProxy(sourceProxyId);
        proxyHeartbeats.remove(sourceProxyId);
    }

    private void sendHeartbeat() {
        final JsonObject heartbeat = new JsonObject();
        heartbeat.addProperty("proxyId", proxyId);
        heartbeat.addProperty("timestamp", System.currentTimeMillis());

        final List<String> playerUuids = plugin.getServer().getAllPlayers().stream()
                .map(p -> p.getUniqueId().toString())
                .toList();
        heartbeat.add("playerUuids", GSON.toJsonTree(playerUuids));

        backend.publishAsync(CHANNEL_HEARTBEAT, GSON.toJson(heartbeat));
    }

    private void cleanStaleProxies() {
        final long now = System.currentTimeMillis();
        final long timeout = settings.getHeartbeatTimeout();

        final List<String> staleProxies = proxyHeartbeats.entrySet().stream()
                .filter(e -> now - e.getValue() > timeout)
                .map(Map.Entry::getKey)
                .toList();

        for (String staleProxyId : staleProxies) {
            plugin.getLogger().warn("[Velocitab] Proxy {} heartbeat timed out, removing its players", staleProxyId);
            plugin.getTabList().getTaskManager().run(() -> removeAllPlayersFromProxy(staleProxyId));
            proxyHeartbeats.remove(staleProxyId);
        }
    }

    private void publishAllPlayerUpdates() {
        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .forEach(this::publishPlayerUpdate);
    }

    private void removeRemotePlayer(@NotNull UUID uuid) {
        removeRemoteFromLocalViewers(uuid);
        plugin.getTabList().getPlayers().remove(uuid);
        remotePlayerProxyMap.remove(uuid);
    }

    private void removeAllPlayersFromProxy(@NotNull String targetProxyId) {
        final List<UUID> toRemove = remotePlayerProxyMap.entrySet().stream()
                .filter(e -> e.getValue().equals(targetProxyId))
                .map(Map.Entry::getKey)
                .toList();

        for (UUID uuid : toRemove) {
            removeRemotePlayer(uuid);
        }
    }

    private void addRemoteToLocalViewers(@NotNull TabPlayer remotePlayer) {
        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .filter(p -> p.getGroup().equals(remotePlayer.getGroup()) ||
                        plugin.getSettings().isShowAllPlayersFromAllGroups())
                .forEach(viewer -> {
                    final var displayName = plugin.getTabList().formatComponent(remotePlayer,
                            plugin.getPlaceholderManager().applyPlaceholders(remotePlayer, remotePlayer.getGroup().format()));
                    viewer.getPlayer().getTabList().addEntry(
                            plugin.getTabList().createEntry(remotePlayer, viewer.getPlayer().getTabList(), displayName)
                    );
                    viewer.sendHeaderAndFooter(plugin.getTabList());
                });

        // Set up scoreboard team for sorting
        if (plugin.getSettings().isSendScoreboardPackets()) {
            plugin.getScoreboardManager().addRemotePlayerTeam(remotePlayer);
        }
    }

    private void removeRemoteFromLocalViewers(@NotNull UUID uuid) {
        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .forEach(viewer -> {
                    viewer.getPlayer().getTabList().removeEntry(uuid);
                    viewer.sendHeaderAndFooter(plugin.getTabList());
                });

        if (plugin.getSettings().isSendScoreboardPackets()) {
            plugin.getScoreboardManager().removeRemotePlayerTeam(uuid);
        }
    }

    private void updateRemoteForLocalViewers(@NotNull TabPlayer remotePlayer) {
        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .filter(p -> p.getGroup().equals(remotePlayer.getGroup()) ||
                        plugin.getSettings().isShowAllPlayersFromAllGroups())
                .forEach(viewer -> {
                    final var displayName = plugin.getTabList().formatComponent(remotePlayer,
                            plugin.getPlaceholderManager().applyPlaceholders(remotePlayer, remotePlayer.getGroup().format()));
                    plugin.getTabList().updateEntryDisplayName(remotePlayer, viewer, displayName);
                });
    }

    @Nullable
    private Group resolveGroup(@NotNull String groupName) {
        return plugin.getTabGroupsManager().getGroup(groupName)
                .orElseGet(() -> plugin.getSettings().isFallbackEnabled()
                        ? plugin.getTabGroupsManager().getGroup(plugin.getSettings().getFallbackGroup()).orElse(null)
                        : null);
    }
}
