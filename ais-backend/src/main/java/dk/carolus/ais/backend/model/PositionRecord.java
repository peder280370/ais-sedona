package dk.carolus.ais.backend.model;

import lombok.Value;

@Value
public class PositionRecord {
    long mmsi;
    String ts;
    String geomWkt;
    Float sog;
    Float cog;
    Integer heading;
    Integer navStatus;
    Float rot;
    Integer msgType;
    // Enriched from vessel metadata cache
    String vesselName;
    Integer shipType;
    String shipTypeDesc;
    Float lengthM;
}
