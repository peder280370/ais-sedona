package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.GeoJsonFeatureCollection;
import dk.carolus.ais.backend.service.SedonaQueryService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

@Slf4j
@Path("/api/tracks")
@Produces(MediaType.APPLICATION_JSON)
public class TracksResource {

    @Inject
    SedonaQueryService queryService;

    @GET
    @Blocking
    @Operation(summary = "Query voyage tracks (GeoJSON)")
    public GeoJsonFeatureCollection getTracks(
            @Parameter(description = "Bounding box minimum longitude") @QueryParam("minLon") Double minLon,
            @Parameter(description = "Bounding box minimum latitude") @QueryParam("minLat") Double minLat,
            @Parameter(description = "Bounding box maximum longitude") @QueryParam("maxLon") Double maxLon,
            @Parameter(description = "Bounding box maximum latitude") @QueryParam("maxLat") Double maxLat,
            @Parameter(description = "Filter by MMSI") @QueryParam("mmsi") Long mmsi,
            @Parameter(description = "Start date (YYYY-MM-DD)") @QueryParam("startDate") String startDate,
            @Parameter(description = "End date (YYYY-MM-DD)") @QueryParam("endDate") String endDate) {
        log.info("GET /api/tracks bbox=[{},{},{},{}] mmsi={} startDate={} endDate={}",
                minLon, minLat, maxLon, maxLat, mmsi, startDate, endDate);
        var result = queryService.queryTracks(minLon, minLat, maxLon, maxLat, mmsi, startDate, endDate);
        log.info("GET /api/tracks returned {} features", result.features().size());
        return result;
    }
}
