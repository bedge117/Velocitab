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

import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.util.GameProfile;
import lombok.Getter;
import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.Group;
import net.william278.velocitab.player.TabPlayer;
import net.william278.velocitab.tab.Nametag;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * Represents a player connected to a remote Velocity proxy instance.
 * Does not hold a live Player reference — all data comes from a {@link PlayerSnapshot}.
 */
public class RemoteTabPlayer extends TabPlayer {

    @Getter
    private final String proxyId;
    private final GameProfile gameProfile;
    private volatile String serverName;
    private volatile int ping;
    private volatile boolean vanished;
    private volatile String storedTeamName;
    @Getter
    private volatile String resolvedDisplayName;
    private volatile String resolvedNametagPrefix;
    private volatile String resolvedNametagSuffix;

    public RemoteTabPlayer(@NotNull Velocitab plugin, @NotNull PlayerSnapshot snapshot, @NotNull Group group) {
        super(plugin, null, snapshot.toRole(), group, false, true);
        this.proxyId = snapshot.getProxyId();
        this.gameProfile = snapshot.toGameProfile();
        this.serverName = snapshot.getServerName();
        this.ping = snapshot.getPing();
        this.vanished = snapshot.isVanished();
        this.storedTeamName = snapshot.getTeamName();
        this.resolvedDisplayName = snapshot.getResolvedDisplayName();
        this.resolvedNametagPrefix = snapshot.getResolvedNametagPrefix();
        this.resolvedNametagSuffix = snapshot.getResolvedNametagSuffix();
        setCustomName(snapshot.getCustomName());
        setLastServer(snapshot.getServerName());
        setListOrder(snapshot.getListOrder());
        setLoaded(true);
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    @NotNull
    public UUID getUniqueId() {
        return gameProfile.getId();
    }

    @Override
    @NotNull
    public String getUsername() {
        return gameProfile.getName();
    }

    @Override
    @NotNull
    public GameProfile getGameProfile() {
        return gameProfile;
    }

    @Override
    public int getPing() {
        return ping;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    @NotNull
    public String getServerName() {
        return serverName;
    }

    @Override
    @NotNull
    public String getTeamName(@NotNull Velocitab plugin) {
        return storedTeamName != null ? storedTeamName : "";
    }

    @Override
    @NotNull
    public Nametag getNametag(@NotNull Velocitab plugin) {
        return new Nametag(
                resolvedNametagPrefix != null ? resolvedNametagPrefix : "",
                resolvedNametagSuffix != null ? resolvedNametagSuffix : ""
        );
    }

    public boolean isVanished() {
        return vanished;
    }

    /**
     * Returns the Player reference. Remote players do not have a local Player object.
     *
     * @throws UnsupportedOperationException always — use accessor methods instead
     */
    @Override
    public Player getPlayer() {
        throw new UnsupportedOperationException(
                "RemoteTabPlayer does not have a local Player reference. Use accessor methods (getUniqueId(), getUsername(), etc.) instead."
        );
    }

    /**
     * Update this remote player's mutable data from a new snapshot.
     */
    public void updateFromSnapshot(@NotNull PlayerSnapshot snapshot) {
        this.serverName = snapshot.getServerName();
        this.ping = snapshot.getPing();
        this.vanished = snapshot.isVanished();
        this.storedTeamName = snapshot.getTeamName();
        this.resolvedDisplayName = snapshot.getResolvedDisplayName();
        this.resolvedNametagPrefix = snapshot.getResolvedNametagPrefix();
        this.resolvedNametagSuffix = snapshot.getResolvedNametagSuffix();
        setRole(snapshot.toRole());
        setCustomName(snapshot.getCustomName());
        setLastServer(snapshot.getServerName());
        setListOrder(snapshot.getListOrder());
    }
}
