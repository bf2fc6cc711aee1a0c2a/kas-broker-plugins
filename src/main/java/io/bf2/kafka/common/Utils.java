package io.bf2.kafka.common;

import com.google.common.collect.Range;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of string is key1:val1,key2:val2 ....
     *
     * @param list the list with the format: key1:val1,key2:val2
     * @return  the unmodifiable map with the {key1=val1, key2=val2}
     */
    public static Map<String, String> parseListToMap(List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new HashMap<>(list.size());

        list.stream().forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            if (delimiter == -1) {
                throw new IllegalArgumentException("The provided config is not in the correct format: config:value");
            }
            map.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });
        return Map.copyOf(map);
    }

    /**
     * This method gets comma separated values which contains key:min:max and returns a map of
     * key range pairs. the format of string is key1:min1:max1,key2:min2:max2 ....
     *
     * @param list the list with the format: key1:min1:max1,key2:min2:max2
     * @return  the unmodifiable map with the {key1=range1, key2=range2}
     */
    public static Map<String, Range> parseListToRangeMap(List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Range> map = new HashMap<>(list.size());

        list.stream().forEach(s -> {
            // split "key:min:max" into [key, min, max]
            String[] parts = s.split(":", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("The provided config is not in the correct format: config:minValue:maxValue");
            }

            String configKey = parts[0].trim();
            String min = parts[1];
            String max = parts[2];

            Double lowerBound;
            Double upperBound;
            // convert the number into double for accurate comparison
            try {
                lowerBound = min.isBlank() ? null : Double.valueOf(min);
                upperBound = max.isBlank() ? null : Double.valueOf(max);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("The provided min or max value is not a number.", e);
            }

            if (lowerBound == null && upperBound == null) {
                throw new IllegalArgumentException("The provided lower bound and upper bound value are empty.");
            } else if (lowerBound == null) {
                map.put(configKey, Range.atMost(upperBound));
            } else if (upperBound == null) {
                map.put(configKey, Range.atLeast(lowerBound));
            } else {
                map.put(configKey, Range.closed(lowerBound, upperBound));
            }
        });
        return Map.copyOf(map);
    }
}
