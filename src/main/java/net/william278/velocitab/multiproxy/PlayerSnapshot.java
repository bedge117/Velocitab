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
import com.velocitypowered.api.util.GameProfile;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.william278.velocitab.player.Role;
import net.william278.velocitab.player.TabPlayer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A serializable snapshot of a player's data for multi-proxy synchronization.
 * Captures all data needed to render a remote player in the TAB list.
 */
@Getter
@AllArgsConstructor
public class PlayerSnapshot {

    private static final Gson GSON = new GsonBuilder().create();

    private final String proxyId;
    private final UUID uuid;
    private final String username;
    private final String serverName;
    private final String groupName;
    private final int roleWeight;
    @Nullable private final String roleName;
    @Nullable private final String roleDisplayName;
    @Nullable private final String rolePrefix;
    @Nullable private final String roleSuffix;
    private final int ping;
    private final boolean vanished;
    @Nullable private final String customName;
    @Nullable private final String teamName;
    private final int listOrder;
    private final SerializedGameProfile profile;
    private final long timestamp;

    /**
     * Capture a snapshot from a local TabPlayer. All reads are from RAM (microseconds).
     */
    @NotNull
    public static PlayerSnapshot capture(@NotNull TabPlayer tabPlayer, @NotNull String proxyId) {
        final Role role = tabPlayer.getRole();
        return new PlayerSnapshot(
                proxyId,
                tabPlayer.getUniqueId(),
                tabPlayer.getUsername(),
                tabPlayer.getServerName(),
                tabPlayer.getGroup().name(),
                role.getWeight(),
                role.getName().orElse(null),
                role.getDisplayName().orElse(null),
                role.getPrefix().orElse(null),
                role.getSuffix().orElse(null),
                tabPlayer.getPing(),
                false,
                tabPlayer.getCustomName().orElse(null),
                tabPlayer.getLastTeamName().orElse(null),
                tabPlayer.getListOrder(),
                SerializedGameProfile.from(tabPlayer.getGameProfile()),
                System.currentTimeMillis()
        );
    }

    @NotNull
    public String toJson() {
        return GSON.toJson(this);
    }

    @NotNull
    public static PlayerSnapshot fromJson(@NotNull String json) {
        return GSON.fromJson(json, PlayerSnapshot.class);
    }

    /**
     * Reconstruct a Role from the snapshot data.
     */
    @NotNull
    public Role toRole() {
        return new Role(roleWeight, roleName, roleDisplayName, rolePrefix, roleSuffix);
    }

    /**
     * Reconstruct a GameProfile from the snapshot data.
     */
    @NotNull
    public GameProfile toGameProfile() {
        return profile.toGameProfile();
    }

    /**
     * Serializable representation of a GameProfile including skin textures.
     */
    @Getter
    @AllArgsConstructor
    public static class SerializedGameProfile {
        private final UUID uuid;
        private final String name;
        private final List<SerializedProperty> properties;

        @NotNull
        public static SerializedGameProfile from(@NotNull GameProfile profile) {
            final List<SerializedProperty> props = profile.getProperties().stream()
                    .map(p -> new SerializedProperty(p.getName(), p.getValue(), p.getSignature()))
                    .collect(Collectors.toList());
            return new SerializedGameProfile(profile.getId(), profile.getName(), props);
        }

        @NotNull
        public GameProfile toGameProfile() {
            final List<GameProfile.Property> props = properties.stream()
                    .map(p -> new GameProfile.Property(p.name, p.value, p.signature))
                    .collect(Collectors.toList());
            return new GameProfile(uuid, name, props);
        }

        @Getter
        @AllArgsConstructor
        public static class SerializedProperty {
            private final String name;
            private final String value;
            @Nullable private final String signature;
        }
    }
}
