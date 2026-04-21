## ADR-001: Entity scope of derived variables in data_model.py

Each module-level variable in `data_model.py` is filtered to a specific set of `Entiteit`
values. These boundaries are intentional and must not be widened without updating this
document and adding regression tests.

1. `cumulative_columns` covers all rows with `Cumulatief == "ja"` and
   `Type variabele != "date"`, with no `Entiteit` filter. This is safe because only
   `Prestatiedata` rows (raw meter readings from providers) carry `Cumulatief == "ja"`
   by data model design. `PrestatiedataBerekend` columns are derived totals and are
   never cumulative meter readings.
2. `model_column_order` and `model_column_type` cover `Entiteit == "Prestatiedata"` only.
   These variables represent what data providers supply in raw files. `PrestatiedataBerekend`
   columns do not exist at the point where these lists are used (ordering and typing of
   incoming mapped parquet files).
3. `data_analysis_columns` is currently an alias for `model_column_order`. When the
   `Vereist` column in `etdmodel.csv` is fully populated, this should be narrowed to
   `Vereist == "ja"` rows. A comment in `data_model.py` marks the transition point.
4. `all_performance_data_columns` covers all rows where `Entiteit` starts with
   `"Prestatiedata"` (i.e. both `"Prestatiedata"` and `"PrestatiedataBerekend"`). This is
   the correct universe when asking "what columns should be present after the full pipeline
   has run?" Use this — not `model_column_order` — when defining derivation targets.
5. `required_performance_data_columns` is the subset of `all_performance_data_columns`
   where `Vereist == "ja"`. These must be present at the end of the pipeline, either
   supplied by the provider or derived by the transform step.

---

## ADR-002: Catalog loading is in etdmap; catalog building is not

The derivation catalog (`catalog.parquet`) is split across two packages by design.

1. **etdmap owns loading**: `etdmap.catalog` provides `load_catalog()`,
   `load_rules()`, and `check_derivability()`. These are pure pandas + set operations
   with no SymPy dependency. `etdmap` must remain safe to import even when the catalog
   has not yet been built or is stale.
2. **etdmap does not own building**: Catalog building (BFS expansion, SymPy parsing,
   Pareto pruning) requires SymPy and lives outside `etdmap`. A broken or missing
   build step must never prevent `etdmap` from loading.
3. `load_catalog()` raises `FileNotFoundError` if `catalog.parquet` is absent, with a
   message pointing to the remediation step. This is a hard failure per ADR-003 of the
   parent project.
4. `load_rules()` loads `Rule.csv` from the package data directory. The module-level
   sets `rule_lhs_variables`, `rule_rhs_variables`, and `rule_all_variables` are
   derived from it at import time and can be used for pre-flight cross-checks.

---

## ADR-003: Catalog-to-model consistency is an invariant

Every LHS target in `catalog.parquet` must correspond to a column in
`all_performance_data_columns`. A rule that targets a column outside the data model is
a data model error, not a runtime condition.

1. Sync tooling must validate this before building the catalog. Any violation is a
   blocking error that prevents the catalog from being written.
2. A test in `etdmap/tests/` must enforce this invariant so that the constraint is
   checked on every run, not only during sync.
3. `add_calculated_columns_adaptive()` does not need to handle the "catalog column
   outside model" case at runtime — the invariant guarantees it cannot occur.
