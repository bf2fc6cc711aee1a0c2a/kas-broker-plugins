package io.bf2.kafka.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a rule that doesn't allow the config value to be updated
 */
public class NotAllowUpdateRule implements ConfigRule {
    private final Set<String> notAllowUpdateConfigs;

    public NotAllowUpdateRule(Set<String> notAllowUpdateConfigs) {
        this.notAllowUpdateConfigs = notAllowUpdateConfigs;
    }


    @Override
    public List<InvalidConfig> validate(String topic, Map<String, String> configs) {
        List<InvalidConfig> invalidConfigs = new ArrayList<>();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (notAllowUpdateConfigs.contains(key)) {
                invalidConfigs.add(new InvalidConfig(key, entry.getValue(), InvalidConfig.RuleType.NOT_ALLOW_UPDATE_RULE));
            }
        }
        return invalidConfigs;
    }
}
