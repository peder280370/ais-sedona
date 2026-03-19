package dk.carolus.ais.io.model;

import lombok.Value;

import java.time.Instant;

/**
 * Immutable value object representing one AIS vessel static data record
 * (decoded from message types 5 or 24).
 *
 * <p>A single MMSI may produce multiple records per run (one per received
 * static message).  Downstream consumers should deduplicate by MMSI,
 * keeping the most-recently-seen record.
 */
@Value
public class VesselMetadata {

    long mmsi;
    Long imo;
    String vesselName;
    String callsign;
    Integer shipType;
    String shipTypeDesc;
    Float lengthM;
    Float beamM;
    Float draughtM;
    String destination;
    Instant lastSeen;
}
