package dk.carolus.ais.backend.model;

public record ImportResult(
        long positionsWritten,
        long vesselRecords,
        long tracksBuilt,
        long durationMs) {
}
