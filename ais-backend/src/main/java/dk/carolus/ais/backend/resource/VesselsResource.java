package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.VesselRecord;
import dk.carolus.ais.backend.service.SedonaQueryService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;

import java.util.List;

@Slf4j
@Path("/api/vessels")
@Produces(MediaType.APPLICATION_JSON)
public class VesselsResource {

    @Inject
    SedonaQueryService queryService;

    @GET
    @Blocking
    @Operation(summary = "Search vessels by MMSI or name")
    public List<VesselRecord> getVessels(
            @QueryParam("mmsi") Long mmsi,
            @QueryParam("name") String name) {
        log.info("GET /api/vessels mmsi={} name={}", mmsi, name);
        var results = queryService.queryVessels(mmsi, name);
        log.info("GET /api/vessels returned {} records", results.size());
        return results;
    }

    @GET
    @Path("/{mmsi}")
    @Blocking
    @Operation(summary = "Get vessel by MMSI")
    public List<VesselRecord> getVessel(@PathParam("mmsi") long mmsi) {
        log.info("GET /api/vessels/{}", mmsi);
        var results = queryService.queryVessels(mmsi, null);
        log.info("GET /api/vessels/{} returned {} records", mmsi, results.size());
        return results;
    }
}
