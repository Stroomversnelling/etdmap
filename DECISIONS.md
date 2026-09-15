## ADR-001: Entity scope of derived variables in data_model.py

Each module-level variable in `data_model.py` is filtered to a specific set of `Entiteit`
values. These boundaries are intentional and must not be widened without updating this
document and adding regression tests.

**The data model is a theoretical model.** It declares which variables exist, what they
mean, and what type they have. It does **not** declare what any supplier actually
delivers. Every `Prestatiedata*` variable is fair game as raw supplied data, whether or
not it is marked as calculated. What a given supplier delivers is recorded in that
supplier's own metadata, not here. `Entiteit` and the `Berekend` suffix are naming
conventions that make derivation rules readable; they are not a statement about the
origin of a value in any particular dataset.

1. `cumulative_columns` covers all rows with `Cumulatief == "ja"` and
   `Type variabele != "date"`, with no `Entiteit` filter. The filter is on the
   `Cumulatief` flag alone, which is the property that matters; no `Entiteit`
   assumption is made or needed.
2. `model_column_order` and `model_column_type` cover `Entiteit == "Prestatiedata"` only.
   This is an **ordering and typing convenience** for the subset that carries a declared
   `Volgorde`, not a statement about which columns a supplier may deliver. Do not use
   membership in these to decide whether a delivered column is legitimate — use
   `performance_column_dtypes`, which spans the whole model.
3. `data_analysis_columns` is currently an alias for `model_column_order`. When the
   `Vereist` column in `etdmodel.csv` is fully populated, this should be narrowed to
   `Vereist == "ja"` rows. A comment in `data_model.py` marks the transition point.
4. `all_performance_data_columns` covers all rows where `Entiteit` starts with
   `"Prestatiedata"` (i.e. both `"Prestatiedata"` and `"PrestatiedataBerekend"`). This is
   the correct universe when asking "what columns may be present at any point in the
   pipeline?" Use this — not `model_column_order` — when defining derivation targets.
5. `performance_column_dtypes` maps every `Prestatiedata*` variable to its dtype, in
   column order (declared `Volgorde` first, then the rest in model order). This is the
   membership test and the ordering key for mapped output: any data model column is
   allowed through regardless of `Entiteit`.
6. `required_performance_data_columns` is the subset of `all_performance_data_columns`
   where `Vereist == "ja"`. These must be present at the end of the pipeline, however
   they got there — delivered, mapped, or derived.

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
