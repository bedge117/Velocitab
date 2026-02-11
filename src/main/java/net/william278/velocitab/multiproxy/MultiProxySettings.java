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

import de.exlll.configlib.Comment;
import de.exlll.configlib.Configuration;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.william278.velocitab.Velocitab;
import net.william278.velocitab.config.ConfigValidator;
import net.william278.velocitab.multiproxy.mysql.MySQLSettings;
import net.william278.velocitab.multiproxy.redis.RedisSettings;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("FieldMayBeFinal")
@Getter
@Configuration
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class MultiProxySettings implements ConfigValidator {

    @Comment("Enable multi-proxy TAB list support for showing players across multiple Velocity proxies")
    private boolean enabled = false;

    @Comment("Backend type for cross-proxy communication: REDIS or MYSQL")
    private BackendType backendType = BackendType.REDIS;

    @Comment({"Unique identifier for this proxy instance.",
            "Set to \"auto\" to generate a random ID on each startup."})
    private String proxyId = "auto";

    @Comment("How often (in milliseconds) to publish player updates to other proxies")
    private long updateInterval = 3000;

    @Comment("How often (in milliseconds) to send heartbeat pings to other proxies")
    private long heartbeatInterval = 10000;

    @Comment("After how many milliseconds without a heartbeat to consider a proxy offline and remove its players")
    private long heartbeatTimeout = 30000;

    @Comment("Redis backend settings (used when backend_type is REDIS)")
    private RedisSettings redis = new RedisSettings();

    @Comment("MySQL/MariaDB backend settings (used when backend_type is MYSQL)")
    private MySQLSettings mysql = new MySQLSettings();

    @Override
    public void validateConfig(@NotNull Velocitab plugin, @NotNull String name) {
        if (updateInterval < 500) {
            throw new IllegalStateException("Multi-proxy update_interval must be at least 500ms");
        }
        if (heartbeatInterval < 1000) {
            throw new IllegalStateException("Multi-proxy heartbeat_interval must be at least 1000ms");
        }
        if (heartbeatTimeout <= heartbeatInterval) {
            throw new IllegalStateException("Multi-proxy heartbeat_timeout must be greater than heartbeat_interval");
        }
    }

    public enum BackendType {
        REDIS,
        MYSQL
    }
}
