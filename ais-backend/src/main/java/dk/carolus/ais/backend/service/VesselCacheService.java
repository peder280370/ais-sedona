package dk.carolus.ais.backend.service;

import dk.carolus.ais.backend.model.PositionRecord;
import dk.carolus.ais.backend.model.VesselRecord;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@ApplicationScoped
public class VesselCacheService {

    @Inject
    SedonaQueryService queryService;

    private volatile Map<Long, VesselRecord> cache = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "vessel-cache-refresh");
        t.setDaemon(true);
        return t;
    });

    @PostConstruct
    void init() {
        refresh();
        scheduler.scheduleAtFixedRate(this::refresh, 5, 5, TimeUnit.MINUTES);
    }

    @PreDestroy
    void destroy() {
        scheduler.shutdownNow();
    }

    void refresh() {
        try {
            List<VesselRecord> vessels = queryService.queryVessels(null, null);
            var updated = new ConcurrentHashMap<Long, VesselRecord>(vessels.size() * 2);
            for (var v : vessels) {
                updated.put(v.mmsi(), v);
            }
            cache = updated;
            log.debug("Vessel cache refreshed: {} entries", updated.size());
        } catch (Exception e) {
            log.warn("Failed to refresh vessel cache", e);
        }
    }

    public List<PositionRecord> enrich(List<PositionRecord> positions) {
        if (cache.isEmpty()) return positions;
        return positions.stream().map(this::enrichOne).toList();
    }

    private PositionRecord enrichOne(PositionRecord p) {
        VesselRecord v = cache.get(p.mmsi());
        if (v == null) return p;
        return new PositionRecord(
            p.mmsi(), p.ts(), p.geomWkt(),
            p.sog(), p.cog(), p.heading(), p.navStatus(), p.rot(), p.msgType(),
            v.vesselName(), v.shipType(), v.shipTypeDesc(), v.lengthM()
        );
    }
}
