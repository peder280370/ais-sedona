# AIS Test Files

Sample NMEA AIS data files for testing the pipeline parser.

All files contain raw `!AIVDM` / `!AIVDO` NMEA sentences parseable by `com.github.tbsalling:aismessages`.

## Files

| File | Lines | Size | Description |
|---|---|---|---|
| `nmea-sample-large.txt` | 85,194 | 4.0 MB | Large corpus for volume testing |
| `sample-800lines.aivdm` | 1,121 | 46 KB | Medium sample from real vessel traffic |
| `mixed_types.nmea` | 108 | 5.1 KB | Covers many AIS message types (1–27) |
| `multi-part.nmea` | 93 | 5.5 KB | Multi-part messages (e.g. Type 5 vessel static data) |
| `sample.nmea` | 2 | 323 B | Minimal example |
| `errors.nmea` | 4 | 459 B | Malformed / edge-case messages |

## Sources

| File | Source URL |
|---|---|
| `nmea-sample-large.txt` | https://raw.githubusercontent.com/M0r13n/pyais/master/tests/nmea-sample |
| `sample-800lines.aivdm` | https://raw.githubusercontent.com/ianfixes/nmea_plus/master/spec/standards/sample.aivdm |
| `mixed_types.nmea` | https://raw.githubusercontent.com/GlobalFishingWatch/ais-tools/master/sample/mixed_types.nmea |
| `multi-part.nmea` | https://raw.githubusercontent.com/GlobalFishingWatch/ais-tools/master/sample/multi-part.nmea |
| `sample.nmea` | https://raw.githubusercontent.com/GlobalFishingWatch/ais-tools/master/sample/sample.nmea |
| `errors.nmea` | https://raw.githubusercontent.com/GlobalFishingWatch/ais-tools/master/sample/errors.nmea |

The large corpus (`nmea-sample-large.txt`) originates from Kurt Schwehr's gpsd test dataset, mirrored in several open-source AIS libraries. The `mixed_types.nmea` and related small files are from Global Fishing Watch's `ais-tools` repository. `sample-800lines.aivdm` is from the `nmea_plus` Ruby library's test suite.
