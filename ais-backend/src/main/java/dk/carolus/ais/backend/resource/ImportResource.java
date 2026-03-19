package dk.carolus.ais.backend.resource;

import dk.carolus.ais.backend.model.ImportResult;
import dk.carolus.ais.backend.service.AisImportService;
import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.jboss.resteasy.reactive.RestForm;
import org.jboss.resteasy.reactive.multipart.FileUpload;
import java.io.InputStream;
import java.time.LocalDate;

@Slf4j
@Path("/api/import")
@Produces(MediaType.APPLICATION_JSON)
public class ImportResource {

    @Inject
    AisImportService importService;

    @POST
    @Path("/file")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Blocking
    @Operation(summary = "Import AIS file (NMEA or CSV)")
    public ImportResult importFile(
            @Parameter(description = "AIS data file to import") @RestForm("file") FileUpload file,
            @Parameter(description = "File format: csv or nmea") @RestForm("format") String format,
            @Parameter(description = "Override date for records (YYYY-MM-DD)") @RestForm("date") String date) throws Exception {
        LocalDate localDate = (date != null && !date.isBlank()) ? LocalDate.parse(date) : null;
        log.info("POST /api/import/file filename={} format={} date={}", file.fileName(), format, localDate);
        try (InputStream is = java.nio.file.Files.newInputStream(file.filePath())) {
            ImportResult result = importService.importFile(is, file.fileName(), format != null ? format : "nmea", localDate);
            log.info("POST /api/import/file complete positions={} vessels={} tracks={} durationMs={}",
                    result.getPositionsWritten(), result.getVesselRecords(), result.getTracksBuilt(), result.getDurationMs());
            return result;
        }
    }

    @POST
    @Path("/dma")
    @Consumes(MediaType.APPLICATION_JSON)
    @Blocking
    @Operation(summary = "Download and import DMA daily AIS data")
    public ImportResult importDma(DmaRequest request) throws Exception {
        log.info("POST /api/import/dma date={}", request.date());
        ImportResult result = importService.importDma(request.date());
        log.info("POST /api/import/dma complete positions={} vessels={} tracks={} durationMs={}",
                result.getPositionsWritten(), result.getVesselRecords(), result.getTracksBuilt(), result.getDurationMs());
        return result;
    }

    public record DmaRequest(String date) {}
}
