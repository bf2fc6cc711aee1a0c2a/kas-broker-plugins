package io.bf2.kafka.common;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {

    @Test
    void parseListToMapShouldReturnEmptyMapWithNullList() {
        assertEquals(Collections.emptyMap(), Config.parseListToMap(null));
    }

    @Test
    void parseListToMapShouldReturnEmptyMapWithEmptyList() {
        assertEquals(Collections.emptyMap(), Config.parseListToMap(Collections.emptyList()));
    }

    @Test
    void parseListToMapShouldReturnExpectedMap() {
        List<String> configList = List.of(
                "x.y.z:123",
                "xx.yy.zz:0.5",
                "xxx.yyy.zzz:abc"
        );
        Map<String, String> expectedMap = Map.of(
                "x.y.z", "123",
                "xx.yy.zz", "0.5",
                "xxx.yyy.zzz", "abc"
        );
        assertEquals(expectedMap, Config.parseListToMap(configList));
    }

    @Test
    void parseListToMapShouldThrowExceptionIfBadFormat() {
        List<String> configList = List.of(
                "x.y.z:123",
                "xx.yy.zz:0.5",
                "bad.format"
        );
        assertThrows(IllegalArgumentException.class, () -> Config.parseListToMap(configList));
    }

    @Test
    void parseListToRangeMapShouldReturnEmptyMapWithNullList() {
        assertEquals(Collections.emptyMap(), Config.parseListToRangeMap(Collections.emptyList()));
    }

    @Test
    void parseListToRangeMapShouldReturnEmptyMapWithEmptyList() {
        assertEquals(Collections.emptyMap(), Config.parseListToRangeMap(Collections.emptyList()));
    }

    @Test
    void parseListToRangeMapShouldReturnExpectedMap() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "xxx.yyy.zzz::500"
        );
        Map<String, Range<Double>> expectedMap = Map.of(
                "x.y.z", Range.closed((double)100, (double)200),
                "xx.yy.zz", Range.atLeast((double)0.5),
                "xxx.yyy.zzz", Range.atMost((double)500)
        );
        assertEquals(expectedMap, Config.parseListToRangeMap(configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfBadFormat() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:123"
        );
        assertThrows(IllegalArgumentException.class, () -> Config.parseListToRangeMap(configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfMinIsNotNumber() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:abc:123"
        );
        assertThrows(IllegalArgumentException.class, () -> Config.parseListToRangeMap(configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfMaxIsNotNumber() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:123:abc"
        );
        assertThrows(IllegalArgumentException.class, () -> Config.parseListToRangeMap(configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfNoMinAndMax() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format::"
        );
        assertThrows(IllegalArgumentException.class, () -> Config.parseListToRangeMap(configList));
    }
}
