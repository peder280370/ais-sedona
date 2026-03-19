package dk.carolus.ais.io.model;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;

/**
 * Immutable value object representing one derived AIS voyage track segment.
 */
public record AisTrack(
        long mmsi,
        String voyageId,
        byte[] geometryWkb,
        Instant startTime,
        Instant endTime,
        int pointCount,
        Float avgSog,
        Float distanceNm) {

    /** Partition key: YYYY-MM string derived from startTime in UTC (e.g. "2024-01"). */
    public String getMonthPartition() {
        return YearMonth.from(startTime.atOffset(ZoneOffset.UTC)).toString();
    }

    @Override
    public String toString() {
        return "AisTrack[mmsi=" + mmsi + ", voyageId=" + voyageId
            + ", pointCount=" + pointCount + ", avgSog=" + avgSog
            + ", distanceNm=" + distanceNm + "]";
    }
}
