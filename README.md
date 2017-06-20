# Jest benchmarks

## Running benchmarks

A running Elasticsearch node with enabled HTTP API is expected on `http://127.0.0.1:9200`.

The URL can be overridden with the `es.host` and `es.port` system properties.

Running the benchmarks of all modules:
```
./gradlew jmh
```

Running the benchmarks of specific modules (e. g. `jest`):
```
./gradlew :jest:jmh
```

Results will be created in the `build/reports/jmh/` sub-directory of each module.
