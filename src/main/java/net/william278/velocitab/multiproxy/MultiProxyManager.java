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
import net.kyori.adventure.text.Component;
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
 * Designed for minimal event loop impact:
 * - All Redis callbacks run on dedicated threads (never Netty)
 * - Batch updates: one Redis message per cycle, not per-player
 * - Delta-only: only publishes players whose display name changed
 * - Single viewer pass: updates all remote data first, then refreshes viewers once
 */
public class MultiProxyManager {

    private static final String CHANNEL_JOIN = "join";
    private static final String CHANNEL_QUIT = "quit";
    private static final String CHANNEL_BATCH_UPDATE = "batch_update";
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
    // Cache last published display name per player for delta detection
    private final Map<UUID, String> lastPublishedDisplayNames;

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
        this.lastPublishedDisplayNames = new ConcurrentHashMap<>();
    }

    /**
     * Enable multi-proxy synchronization.
     */
    public void enable() {
        backend.connect();

        // Subscribe to all channels — callbacks already run off Netty (RedisBackend dispatches to its own executor)
        backend.subscribe(CHANNEL_JOIN, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteJoin(message)));
        backend.subscribe(CHANNEL_QUIT, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteQuit(message)));
        backend.subscribe(CHANNEL_BATCH_UPDATE, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleBatchUpdate(message)));
        backend.subscribe(CHANNEL_HEARTBEAT, (channel, message) ->
                handleHeartbeat(message));
        backend.subscribe(CHANNEL_SHUTDOWN, (channel, message) ->
                plugin.getTabList().getTaskManager().run(() -> handleRemoteShutdown(message)));

        // Start heartbeat and update tasks on a dedicated thread (not Netty, not TaskManager)
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "Velocitab MultiProxy Heartbeat");
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat,
                settings.getHeartbeatInterval(), settings.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

        heartbeatExecutor.scheduleAtFixedRate(this::cleanStaleProxies,
                settings.getHeartbeatTimeout(), settings.getHeartbeatInterval(), TimeUnit.MILLISECONDS);

        heartbeatExecutor.scheduleAtFixedRate(this::publishBatchUpdate,
                settings.getUpdateInterval(), settings.getUpdateInterval(), TimeUnit.MILLISECONDS);

        plugin.getLogger().info("[Velocitab] Multi-proxy enabled (id: {}, backend: {}, update_interval: {}ms)",
                proxyId, settings.getBackendType(), settings.getUpdateInterval());
    }

    /**
     * Disable multi-proxy synchronization.
     */
    public void disable() {
        final JsonObject shutdown = new JsonObject();
        shutdown.addProperty("proxyId", proxyId);
        backend.publishAsync(CHANNEL_SHUTDOWN, GSON.toJson(shutdown));

        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        backend.disconnect();
        plugin.getLogger().info("[Velocitab] Multi-proxy disabled");
    }

    // ==================== PUBLISH (outbound) ====================

    /**
     * Publish a player join event (individual, real-time).
     */
    public void publishPlayerJoin(@NotNull TabPlayer tabPlayer) {
        final String resolved = resolveDisplayName(tabPlayer);
        lastPublishedDisplayNames.put(tabPlayer.getUniqueId(), resolved);
        final String[] nametag = resolveNametag(tabPlayer);
        final PlayerSnapshot snapshot = PlayerSnapshot.capture(tabPlayer, proxyId, resolved, nametag[0], nametag[1]);
        backend.publishAsync(CHANNEL_JOIN, snapshot.toJson());
    }

    /**
     * Publish a player quit event (individual, real-time).
     */
    public void publishPlayerQuit(@NotNull UUID uuid) {
        lastPublishedDisplayNames.remove(uuid);
        final JsonObject quit = new JsonObject();
        quit.addProperty("proxyId", proxyId);
        quit.addProperty("uuid", uuid.toString());
        backend.publishAsync(CHANNEL_QUIT, GSON.toJson(quit));
    }

    /**
     * Publish a single player update (used for server switch, immediate events).
     */
    public void publishPlayerUpdate(@NotNull TabPlayer tabPlayer) {
        final String resolved = resolveDisplayName(tabPlayer);
        lastPublishedDisplayNames.put(tabPlayer.getUniqueId(), resolved);
        final String[] nametag = resolveNametag(tabPlayer);
        final PlayerSnapshot snapshot = PlayerSnapshot.capture(tabPlayer, proxyId, resolved, nametag[0], nametag[1]);
        // Send as a batch of 1 for consistency
        final JsonObject batch = new JsonObject();
        batch.addProperty("proxyId", proxyId);
        batch.add("players", GSON.toJsonTree(List.of(snapshot)));
        backend.publishAsync(CHANNEL_BATCH_UPDATE, GSON.toJson(batch));
    }

    /**
     * Batch publish: one Redis message with ALL changed players.
     * Delta-only — skips players whose display name hasn't changed.
     */
    private void publishBatchUpdate() {
        final List<PlayerSnapshot> changed = new ArrayList<>();

        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .forEach(player -> {
                    final String resolved = resolveDisplayName(player);
                    final String previous = lastPublishedDisplayNames.get(player.getUniqueId());

                    // Only include if display name changed or first time
                    if (previous == null || !previous.equals(resolved)) {
                        lastPublishedDisplayNames.put(player.getUniqueId(), resolved);
                        final String[] nametag = resolveNametag(player);
                        changed.add(PlayerSnapshot.capture(player, proxyId, resolved, nametag[0], nametag[1]));
                    }
                });

        if (changed.isEmpty()) {
            return; // Nothing changed — no Redis message at all
        }

        final JsonObject batch = new JsonObject();
        batch.addProperty("proxyId", proxyId);
        batch.add("players", GSON.toJsonTree(changed));
        backend.publishAsync(CHANNEL_BATCH_UPDATE, GSON.toJson(batch));
    }

    // ==================== RECEIVE (inbound) ====================

    private void handleRemoteJoin(@NotNull String message) {
        final PlayerSnapshot snapshot = PlayerSnapshot.fromJson(message);
        if (proxyId.equals(snapshot.getProxyId())) {
            return;
        }

        final Group group = resolveGroup(snapshot.getGroupName());
        if (group == null) {
            return;
        }

        final UUID uuid = snapshot.getUuid();
        final TabPlayer existing = plugin.getTabList().getPlayers().get(uuid);
        if (existing instanceof RemoteTabPlayer remote) {
            remote.updateFromSnapshot(snapshot);
            remote.setGroup(group);
        } else {
            final RemoteTabPlayer remote = new RemoteTabPlayer(plugin, snapshot, group);
            plugin.getTabList().getPlayers().put(uuid, remote);
            remotePlayerProxyMap.put(uuid, snapshot.getProxyId());
        }

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

    /**
     * Handle a batch update: process ALL snapshots first, then do ONE viewer refresh pass.
     */
    private void handleBatchUpdate(@NotNull String message) {
        final JsonObject json = JsonParser.parseString(message).getAsJsonObject();
        final String sourceProxyId = json.get("proxyId").getAsString();
        if (proxyId.equals(sourceProxyId)) {
            return;
        }

        final List<PlayerSnapshot> snapshots = GSON.fromJson(
                json.get("players"), new TypeToken<List<PlayerSnapshot>>() {}.getType()
        );
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }

        // Phase 1: Update all RemoteTabPlayer data objects (cheap, no packets)
        final List<RemoteTabPlayer> updatedPlayers = new ArrayList<>();
        final List<PlayerSnapshot> newPlayers = new ArrayList<>();

        for (PlayerSnapshot snapshot : snapshots) {
            final UUID uuid = snapshot.getUuid();
            final TabPlayer existing = plugin.getTabList().getPlayers().get(uuid);

            if (existing instanceof RemoteTabPlayer remote) {
                final Group newGroup = resolveGroup(snapshot.getGroupName());
                if (newGroup == null) {
                    continue;
                }
                final boolean groupChanged = !remote.getGroup().equals(newGroup);
                remote.updateFromSnapshot(snapshot);

                if (groupChanged) {
                    removeRemoteFromLocalViewers(uuid);
                    remote.setGroup(newGroup);
                    addRemoteToLocalViewers(remote);
                } else {
                    updatedPlayers.add(remote);
                }
            } else {
                newPlayers.add(snapshot);
            }
        }

        // Phase 2: Add any new players (not yet tracked)
        for (PlayerSnapshot snapshot : newPlayers) {
            handleRemoteJoin(snapshot.toJson());
        }

        // Phase 3: Single pass to refresh display names for updated players
        if (!updatedPlayers.isEmpty()) {
            refreshDisplayNamesForViewers(updatedPlayers);
        }
    }

    /**
     * Single pass: for each local viewer, update display names for all changed remote players.
     * Much cheaper than iterating viewers per-player.
     */
    private void refreshDisplayNamesForViewers(@NotNull List<RemoteTabPlayer> remotePlayers) {
        final List<TabPlayer> localViewers = plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .toList();

        for (RemoteTabPlayer remote : remotePlayers) {
            final String resolvedText = remote.getResolvedDisplayName() != null
                    ? remote.getResolvedDisplayName()
                    : "";
            final Component displayName = plugin.getTabList().formatComponent(remote, resolvedText);

            for (TabPlayer viewer : localViewers) {
                if (!viewer.getGroup().equals(remote.getGroup()) &&
                        !plugin.getSettings().isShowAllPlayersFromAllGroups()) {
                    continue;
                }
                plugin.getTabList().updateEntryDisplayName(remote, viewer, displayName);
            }
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

    // ==================== INTERNAL ====================

    @NotNull
    private String resolveDisplayName(@NotNull TabPlayer tabPlayer) {
        final String format = tabPlayer.getGroup().format();
        return plugin.getPlaceholderManager().applyPlaceholders(tabPlayer, format);
    }

    @NotNull
    private String[] resolveNametag(@NotNull TabPlayer tabPlayer) {
        final String prefix = plugin.getPlaceholderManager().applyPlaceholders(
                tabPlayer, tabPlayer.getGroup().nametag().prefix());
        final String suffix = plugin.getPlaceholderManager().applyPlaceholders(
                tabPlayer, tabPlayer.getGroup().nametag().suffix());
        return new String[]{prefix, suffix};
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
        final String resolvedText = (remotePlayer instanceof RemoteTabPlayer remote && remote.getResolvedDisplayName() != null)
                ? remote.getResolvedDisplayName()
                : "";
        final Component displayName = plugin.getTabList().formatComponent(remotePlayer, resolvedText);

        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .filter(p -> p.getGroup().equals(remotePlayer.getGroup()) ||
                        plugin.getSettings().isShowAllPlayersFromAllGroups())
                .forEach(viewer -> {
                    viewer.getPlayer().getTabList().addEntry(
                            plugin.getTabList().createEntry(remotePlayer, viewer.getPlayer().getTabList(), displayName)
                    );
                });

        if (plugin.getSettings().isSendScoreboardPackets()) {
            plugin.getScoreboardManager().addRemotePlayerTeam(remotePlayer);
        }
    }

    private void removeRemoteFromLocalViewers(@NotNull UUID uuid) {
        plugin.getTabList().getPlayers().values().stream()
                .filter(p -> !p.isRemote() && p.isLoaded())
                .forEach(viewer -> viewer.getPlayer().getTabList().removeEntry(uuid));

        if (plugin.getSettings().isSendScoreboardPackets()) {
            plugin.getScoreboardManager().removeRemotePlayerTeam(uuid);
        }
    }

    @Nullable
    private Group resolveGroup(@NotNull String groupName) {
        return plugin.getTabGroupsManager().getGroup(groupName)
                .orElseGet(() -> plugin.getSettings().isFallbackEnabled()
                        ? plugin.getTabGroupsManager().getGroup(plugin.getSettings().getFallbackGroup()).orElse(null)
                        : null);
    }
}
