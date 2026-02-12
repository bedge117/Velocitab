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

package net.william278.velocitab.util;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MiniMessageUtil {

    @Getter
    private static final MiniMessageUtil INSTANCE = new MiniMessageUtil();

    private final Pattern legacyRGBPattern = Pattern.compile("[#§x][&§][0-9a-fA-F]{6}");
    private final Pattern legacyAmpersandPattern = Pattern.compile("&[0-9a-fA-FklmnorKLMNOR]");
    private final Pattern legacySectionPattern = Pattern.compile("§[0-9a-fA-FklmnorKLMNOR]");

    private static final Map<Character, String> LEGACY_TO_MINI = Map.ofEntries(
            Map.entry('0', "<black>"), Map.entry('1', "<dark_blue>"), Map.entry('2', "<dark_green>"),
            Map.entry('3', "<dark_aqua>"), Map.entry('4', "<dark_red>"), Map.entry('5', "<dark_purple>"),
            Map.entry('6', "<gold>"), Map.entry('7', "<gray>"), Map.entry('8', "<dark_gray>"),
            Map.entry('9', "<blue>"), Map.entry('a', "<green>"), Map.entry('b', "<aqua>"),
            Map.entry('c', "<red>"), Map.entry('d', "<light_purple>"), Map.entry('e', "<yellow>"),
            Map.entry('f', "<white>"), Map.entry('k', "<obfuscated>"), Map.entry('l', "<bold>"),
            Map.entry('m', "<strikethrough>"), Map.entry('n', "<underlined>"), Map.entry('o', "<italic>"),
            Map.entry('r', "<reset>")
    );

    private MiniMessageUtil() {
    }

    @NotNull
    public String checkForErrors(@NotNull String text) {
        String copy = text;
        copy = convertLegacyRGB(copy);
        copy = convertLegacyCodes(copy, legacySectionPattern);
        copy = convertLegacyCodes(copy, legacyAmpersandPattern);
        return copy;
    }

    @NotNull
    private String convertLegacyRGB(@NotNull String copy) {
        final StringBuilder result = new StringBuilder();
        final Matcher matcher = legacyRGBPattern.matcher(copy);

        while (matcher.find()) {
            String matched = matcher.group();
            // Extract the 6 hex digits from the end
            String hex = matched.substring(matched.length() - 6);
            matcher.appendReplacement(result, Matcher.quoteReplacement("<color:#" + hex + ">"));
        }

        matcher.appendTail(result);
        return result.toString();
    }

    @NotNull
    private String convertLegacyCodes(@NotNull String copy, @NotNull Pattern pattern) {
        final StringBuilder result = new StringBuilder();
        final Matcher matcher = pattern.matcher(copy);

        while (matcher.find()) {
            String matched = matcher.group();
            char code = Character.toLowerCase(matched.charAt(1));
            String replacement = LEGACY_TO_MINI.getOrDefault(code, matched);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(result);
        return result.toString();
    }

}
