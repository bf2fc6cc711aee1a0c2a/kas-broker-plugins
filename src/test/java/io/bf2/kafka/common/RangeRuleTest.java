package io.bf2.kafka.common;

import com.google.common.collect.Range;
import io.bf2.kafka.common.rule.RangeRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RangeRuleTest {
    private RangeRule rangeRule;
    private Map<String, Range<Double>> rangeConfigs = Map.of(
            "x.y.a", Range.atLeast((double)1),
            "x.y.b", Range.atLeast((double)1.0),
            "x.y.c", Range.atLeast((double)1.00),
            "x.z.a", Range.atMost((double)100),
            "x.z.b", Range.atMost((double)0.5),
            "y.z.a", Range.closed((double)0, (double)1),
            "y.z.b", Range.closed((double)1.0, (double)2.0)
    );

    @BeforeEach
    private void setup() {
        rangeRule = new RangeRule(rangeConfigs);
    }

    @ParameterizedTest
    @CsvSource({
            // "x.y.a" allow value >= 1
            "x.y.a, 1, true",
            "x.y.a, 1.0, true",
            "x.y.a, 1.00, true",
            "x.y.a, 1.01, true",
            "x.y.a, 0.99, false",
            // "x.y.b" allow value >= 1.0
            "x.y.b, 1, true",
            "x.y.b, 1.0, true",
            "x.y.b, 1.00, true",
            "x.y.b, 1.01, true",
            "x.y.b, 0.99, false",
            // "x.y.c" allow value >= 1.00
            "x.y.c, 1, true",
            "x.y.c, 1.0, true",
            "x.y.c, 1.00, true",
            "x.y.c, 1.01, true",
            "x.y.c, 0.99, false",
            // "x.z.a" allow value <= 100
            "x.z.a, 100, true",
            "x.z.a, 0, true",
            "x.z.a, 1000, false",
            // "x.z.b" allow value <= 0.5
            "x.z.b, 0.5, true",
            "x.z.b, 0.49, true",
            "x.z.b, 0.50, true",
            "x.z.b, 0.51, false",
            // "y.z.a" allow 0 <= value <= 1
            "y.z.a, 1, true",
            "y.z.a, 0, true",
            "y.z.a, 0.0, true",
            "y.z.a, 1.0, true",
            "y.z.a, 0.5, true",
            "y.z.a, 1.5, false",
            "y.z.a, 2, false",
            // "y.z.b" allow 1.0 <= value <= 2.0
            "y.z.b, 1, true",
            "y.z.b, 2, true",
            "y.z.b, 3, false",
    })
    void testGetDefaultRuleConfigs(String key, String val, boolean isValid) {
        if (isValid) {
            assertTrue(rangeRule.validate(key, val).isEmpty());
        } else {
            assertTrue(rangeRule.validate(key, val).isPresent());
        }

    }
}
