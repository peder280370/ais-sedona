package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.PositionRecord;
import dk.carolus.ais.backend.service.SedonaQueryService;
import dk.carolus.ais.backend.service.VesselCacheService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import java.util.List;

@Slf4j
@Path("/api/positions")
@Produces(MediaType.APPLICATION_JSON)
public class PositionsResource {

    @Inject
    SedonaQueryService queryService;

    @Inject
    VesselCacheService vesselCache;

    @GET
    @Blocking
    @Operation(summary = "Query AIS positions")
    public List<PositionRecord> getPositions(
            @Parameter(description = "Bounding box minimum longitude") @QueryParam("minLon") Double minLon,
            @Parameter(description = "Bounding box minimum latitude") @QueryParam("minLat") Double minLat,
            @Parameter(description = "Bounding box maximum longitude") @QueryParam("maxLon") Double maxLon,
            @Parameter(description = "Bounding box maximum latitude") @QueryParam("maxLat") Double maxLat,
            @Parameter(description = "Filter by MMSI") @QueryParam("mmsi") Long mmsi,
            @Parameter(description = "Point-in-time datetime (ISO, e.g. 2024-01-15T12:30:00)") @QueryParam("at") String at,
            @Parameter(description = "Start date (YYYY-MM-DD)") @QueryParam("startDate") String startDate,
            @Parameter(description = "End date (YYYY-MM-DD)") @QueryParam("endDate") String endDate,
            @Parameter(description = "Maximum number of results") @QueryParam("limit") @DefaultValue("1000") int limit) {
        List<PositionRecord> results;
        if (at != null && !at.isBlank()) {
            log.info("GET /api/positions bbox=[{},{},{},{}] mmsi={} at={} limit={}",
                    minLon, minLat, maxLon, maxLat, mmsi, at, limit);
            results = queryService.queryPositionsAt(minLon, minLat, maxLon, maxLat, mmsi, at, limit);
        } else {
            log.info("GET /api/positions bbox=[{},{},{},{}] mmsi={} startDate={} endDate={} limit={}",
                    minLon, minLat, maxLon, maxLat, mmsi, startDate, endDate, limit);
            results = queryService.queryPositions(minLon, minLat, maxLon, maxLat, mmsi, startDate, endDate, limit);
        }
        results = vesselCache.enrich(results);
        log.info("GET /api/positions returned {} records", results.size());
        return results;
    }
}
