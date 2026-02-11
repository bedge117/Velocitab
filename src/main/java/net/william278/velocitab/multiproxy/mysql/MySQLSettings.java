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

import de.exlll.configlib.Comment;
import de.exlll.configlib.Configuration;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@SuppressWarnings("FieldMayBeFinal")
@Getter
@Configuration
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class MySQLSettings {

    @Comment("MySQL/MariaDB server host")
    private String host = "localhost";

    @Comment("MySQL/MariaDB server port")
    private int port = 3306;

    @Comment("Database name")
    private String database = "velocitab";

    @Comment("Database username")
    private String username = "velocitab";

    @Comment("Database password")
    private String password = "";

    @Comment("How often (in milliseconds) to poll for updates from other proxies")
    private long pollInterval = 2000;

    @Comment("HikariCP connection pool size")
    private int poolSize = 5;

    @Comment("Connection timeout in milliseconds")
    private long connectionTimeout = 5000;

    @Comment("Table name prefix")
    private String tablePrefix = "velocitab_";
}
