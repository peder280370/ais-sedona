package dk.carolus.ais.backend.model;

import lombok.Value;

@Value
public class VesselRecord {
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
    String lastSeen;
}
