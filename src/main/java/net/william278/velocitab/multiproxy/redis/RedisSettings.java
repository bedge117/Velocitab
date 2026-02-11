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

import de.exlll.configlib.Comment;
import de.exlll.configlib.Configuration;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@SuppressWarnings("FieldMayBeFinal")
@Getter
@Configuration
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class RedisSettings {

    @Comment("Redis server host")
    private String host = "localhost";

    @Comment("Redis server port")
    private int port = 6379;

    @Comment("Redis authentication password (leave empty for no authentication)")
    private String password = "";

    @Comment("Redis database index")
    private int database = 0;

    @Comment("Whether to use SSL/TLS for the Redis connection")
    private boolean useSsl = false;

    @Comment("Enable Redis Sentinel mode for high-availability setups")
    private boolean sentinel = false;

    @Comment("Redis Sentinel master name")
    private String sentinelMaster = "mymaster";

    @Comment("Redis Sentinel node addresses (host:port)")
    private List<String> sentinelNodes = List.of("localhost:26379");

    @Comment("Channel prefix for PubSub messages")
    private String channelPrefix = "velocitab";

    @Comment("Connection timeout in milliseconds")
    private long connectionTimeoutMs = 10000;

    @Comment("Command timeout in milliseconds (per-command timeout after dispatch)")
    private long commandTimeoutMs = 5000;
}
