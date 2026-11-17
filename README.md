# Real-time server telemetry pipeline on Azure Databricks

A streaming pipeline for server telemetry. Events come in through Azure Event Hubs,
get cleaned and enriched against a server inventory, and end up as per-datacenter KPIs
that feed a Power BI dashboard.

It started as my Data Engineering capstone. I kept working on it afterwards because I
wanted to actually run the Delta features I had only read about: Z-ordering, time travel,
cloning, vacuum.

## Architecture

```
  Event Hubs  ──────────────┐
  (server telemetry, JSON)  │
                            ▼
  inventory.csv  ──────► [ BRONZE ]   append only, no cleaning
  error_defs.json ──────►    │
                             ▼
                          [ SILVER ]  parse JSON, flatten, drop bad rows,
                             │        stream-static join with inventory
                             ▼
                          [ GOLD ]    watermark + windowed aggregation
                             │        by location and datacenter
                             ▼
                          Power BI
```

One ADLS Gen2 account holds everything. There is a container per layer
(`bronze`, `silver`, `gold`) plus a `checkpoint` container for Auto Loader schemas
and stream state.

| Layer | Table | What's in it |
|---|---|---|
| bronze | `bronze_server_telemetry` | raw Event Hubs payloads |
| bronze | `bronze_server_inventory` | 50 servers, loaded from CSV |
| bronze | `bronze_error_definitions` | nested JSON error catalogue |
| silver | `silver_server_telemetry` | flattened telemetry, joined with location and datacenter |
| gold | `gold_datacenter_stats` | 1-minute KPIs per datacenter |

## How each stage works

**Bronze** uses Auto Loader (`cloudFiles`) for the two file sources, with
`pathGlobFilter` so a single landing folder can hold different file types. Event Hubs
is read with the streaming Event Hubs connector. Nothing is parsed at this layer, which
is the point: if my parsing logic is wrong later I can replay from bronze instead of
asking the source for data again.

**Silver** reads the raw payload as a struct, pulls out `cpu_usage`, `ram_usage`,
`temperature` and `status`, then filters:

```python
df_clean = df_flattened.filter(
    (col("cpu_usage") >= 0) & (col("cpu_usage") <= 100) & (col("server_id").isNotNull())
)
```

That filter exists because the generator deliberately injects about 5% malformed rows
(`cpu_usage = -999`) to prove the pipeline can survive bad input. The inventory table
is small so it is cached and joined as a static side of a stream-static join.

**Gold** aggregates over a sliding window:

- watermark on `generated_time`: 2 minutes
- window: 1 minute wide, sliding every 10 seconds
- grouped by `location` and `data_center_id`
- outputs `avg_cpu`, `max_cpu`, `avg_temp`, `total_logs`
- written in `complete` mode with a 10 second trigger

`max_cpu` is the column I actually care about. Averages hide spikes, and a datacenter
sitting at 60% average with a 99% peak is a different operational problem than one
sitting flat at 60%.

## Maintenance checks

Notebook `04` runs four things I wanted to see work rather than just read about:

1. **Z-ordering.** `OPTIMIZE gold_datacenter_stats ZORDER BY (location, data_center_id)`,
   with query timing measured before and after.
2. **Vacuum.** Removes files the Delta log no longer references, with a size report
   before and after.
3. **Time travel.** Simulated scenario: someone deletes rows by accident. Restore from
   an earlier table version. This is the feature that convinced me to use Delta over
   plain Parquet.
4. **Deep clone.** Clone `silver_server_telemetry` so a risky change can be tested
   without touching the real table, then compare row counts.

## Dashboard

`dashboard/PowerBI*` connects to `gold_datacenter_stats` and shows CPU and temperature
per datacenter over time. The `.pbix` is the working file, screenshots are in the report.

## Running it

1. Create an ADLS Gen2 account with `bronze`, `silver`, `gold`, `checkpoint` containers.
2. Create an Event Hubs namespace and a hub named `server-telemetry`.
3. Set these variables in the cluster environment (or a secret scope, see below):
   `EVENTHUB_SAS_KEY`, `AZURE_STORAGE_KEY`.
4. Run `src/data_generator.py` on your machine to push inventory, error definitions and
   live telemetry.
5. Run the notebooks in order: `01`, `02`, `03`, `04`.

## What is not great about this

Listing this honestly because it is the part I would fix first:

- **Secrets.** They used to be hardcoded in the notebooks. I moved them to environment
  variables, but environment variables on a cluster are still not good. They belong in a
  Databricks secret scope or Key Vault. If you forked this before that change, rotate
  the keys.
- **The Z-order numbers are not a real benchmark.** Everything ran on a single-node
  cluster against a table of a few thousand rows. The timing difference is real but it
  says almost nothing about how it would behave at production volume.
- **The generator is not realistic.** Five events per loop with no pacing. Real telemetry
  is bursty and comes from thousands of hosts, not fifty.
- **Data quality is one filter.** No expectations framework, no quarantine table. Bad
  rows are dropped silently, which means you cannot tell whether a source broke or the
  data was genuinely invalid.
- **`bronze_error_definitions` is barely used.** It is loaded and queried in a couple of
  `%sql` cells at the end of notebook 03, but it is not joined into silver. It should be,
  so each alert carries a severity and a suggested fix.
- **No retention or compaction policy on gold.** It grows forever in its current form.
- **Nothing tests recovery.** I never killed the stream mid-write to see what the
  checkpoint does.

## If I kept going

- Move secrets to a secret scope, remove them from history entirely
- Join error definitions into silver so the dashboard can colour by severity
- Partition gold by date and set a retention policy
- Replace the single filter with an expectations library and a dead-letter table
- Add one test that runs the silver transform over a fixed sample and asserts the output schema

## Repo layout

```
notebooks/   Databricks notebooks, .ipynb so the code is readable on GitHub
exports/     original Databricks HTML exports, these have the cell outputs
src/         data generator, inventory CSV, error definitions
dashboard/   Power BI file and workflow diagram
report/      write-up per stage
```
