package io.bf2.kafka.authorizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConfigHelper {

    public static Map<String, Object> getConfig(Class<?> owner) {
        return getConfig(owner, null);
    }

    public static Map<String, Object> getConfig(Class<?> owner, String suffix, String... additionalConfig) {
        Properties properties = new Properties();
        StringBuilder resourceName = new StringBuilder(owner.getSimpleName());

        if (suffix != null) {
            resourceName.append('-');
            resourceName.append(suffix);
        }

        resourceName.append(".properties");

        try (InputStream stream = owner.getResourceAsStream(resourceName.toString())) {
            properties.load(stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        for (String config : additionalConfig) {
            String[] keyValue = config.split("=");
            properties.setProperty(keyValue[0], keyValue[1]);
        }

        return properties.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
    }

}
