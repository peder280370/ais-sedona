package dk.carolus.ais.io.pipeline;

import dk.carolus.ais.io.model.AisPosition;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates and deduplicates a list of raw {@link AisPosition} records.
 *
 * <p>Validation rules:
 * <ul>
 *   <li>Drop lat outside [-90, 90] or lon outside [-180, 180]</li>
 *   <li>Drop null-island positions (lat=0, lon=0)</li>
 *   <li>Drop SOG &gt; 102.2 knots</li>
 *   <li>Filter MMSI outside [100000000, 999999999]</li>
 *   <li>Drop duplicate (mmsi, timestamp) pairs — keep first occurrence</li>
 * </ul>
 */
@Slf4j
public class Validator {

    private static final double MAX_SOG = 102.2;
    private static final long MMSI_MIN = 100_000_000L;
    private static final long MMSI_MAX = 999_999_999L;

    public static final class Stats {
        public int total;
        public int invalidMmsi;
        public int invalidCoords;
        public int nullIsland;
        public int sogExceeded;
        public int duplicates;
        public int accepted;

        @Override
        public String toString() {
            return String.format(
                    "total=%d  accepted=%d  rejected=[mmsi=%d coords=%d nullIsland=%d sog=%d dups=%d]",
                    total, accepted, invalidMmsi, invalidCoords, nullIsland, sogExceeded, duplicates);
        }
    }

    private final Stats stats = new Stats();
    // Compact dedup key: mmsi (30 bits) in upper 32 bits, epochSecond in lower 32 bits.
    // Uses Long to avoid String allocation overhead for large files.
    private final Set<Long> seen = new HashSet<>();

    /**
     * Streaming variant: tests a single position against all validation rules and the
     * per-instance deduplication set. Safe to call repeatedly on the same instance.
     *
     * @return {@code true} if the position passes all rules and should be kept
     */
    public boolean accept(AisPosition p) {
        stats.total++;
        if (!isValidMmsi(p.mmsi())) { stats.invalidMmsi++; return false; }
        if (!isValidCoords(p.lat(), p.lon())) { stats.invalidCoords++; return false; }
        if (isNullIsland(p.lat(), p.lon())) { stats.nullIsland++; return false; }
        if (isSOGExceeded(p.sog())) { stats.sogExceeded++; return false; }

        long dedupKey = (p.mmsi() << 32) | (p.timestamp().getEpochSecond() & 0xFFFFFFFFL);
        if (!seen.add(dedupKey)) { stats.duplicates++; return false; }

        stats.accepted++;
        return true;
    }

    public List<AisPosition> validate(List<AisPosition> raw) {
        var valid = new ArrayList<AisPosition>(raw.size());
        for (var p : raw) {
            if (accept(p)) valid.add(p);
        }
        log.info("Validation: {}", stats);
        return valid;
    }

    public Stats getStats() {
        return stats;
    }

    // ---- Rule predicates --------------------------------------------------

    static boolean isValidMmsi(long mmsi) {
        return mmsi >= MMSI_MIN && mmsi <= MMSI_MAX;
    }

    static boolean isValidCoords(double lat, double lon) {
        return lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0;
    }

    static boolean isNullIsland(double lat, double lon) {
        return lat == 0.0 && lon == 0.0;
    }

    static boolean isSOGExceeded(Float sog) {
        return sog != null && sog > MAX_SOG;
    }
}
