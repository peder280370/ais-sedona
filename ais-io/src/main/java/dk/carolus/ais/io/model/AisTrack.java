package dk.carolus.ais.io.model;

import lombok.ToString;
import lombok.Value;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;

/**
 * Immutable value object representing one derived AIS voyage track segment.
 */
@Value
public class AisTrack {

    long mmsi;
    String voyageId;
    @ToString.Exclude
    byte[] geometryWkb;
    Instant startTime;
    Instant endTime;
    int pointCount;
    Float avgSog;
    Float distanceNm;

    /** Partition key: YYYY-MM string derived from startTime in UTC (e.g. "2024-01"). */
    public String getMonthPartition() {
        return YearMonth.from(startTime.atOffset(ZoneOffset.UTC)).toString();
    }
}
