package dk.carolus.ais.backend.model;

import lombok.Value;

@Value
public class ImportResult {
    long positionsWritten;
    long vesselRecords;
    long tracksBuilt;
    long durationMs;
}
