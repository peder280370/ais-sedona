package dk.carolus.ais.io.model;

import lombok.Value;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Immutable value object representing one decoded and validated AIS position record.
 */
@Value
public class AisPosition {

    long mmsi;
    Instant timestamp;
    double lat;
    double lon;
    Float sog;
    Float cog;
    Integer heading;
    Integer navStatus;
    Float rot;
    Integer msgType;

    /** Partition key: ISO date string in UTC (e.g. "2024-01-15"). */
    public String getDatePartition() {
        return LocalDate.ofInstant(timestamp, ZoneOffset.UTC).toString();
    }
}
