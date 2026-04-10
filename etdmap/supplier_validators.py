"""
supplier_validators.py — Generic pre-flight validation for supplier column mappings.

Validates that every raw column in a supplier's data file has an explicit
Mapping_Meenemen entry in the ETD DatamodelLeverancier CSV, before any processing starts.

Rules per raw column
--------------------
Mapping_Meenemen == 1, Mapping_VariabeleBSV non-empty  → OK (finalized, will be mapped)
Mapping_Meenemen == 1, Mapping_VariabeleBSV empty      → ERROR (says "include" but no target)
Mapping_Meenemen == 0                                  → OK (explicitly excluded, no mapping needed)
Mapping_Meenemen missing / NaN                         → ERROR (coverage undefined — must be decided)
Column not found in CSV at all                         → ERROR (not covered at all)

The function returns a list of human-readable error strings. An empty list means
all columns are covered. The caller decides whether to raise or log.
"""

import logging
from pathlib import Path

import pandas as pd


def _load_mapping_df(csv_path) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"[supplier_validators] Mapping CSV not found: {path}"
        )
    return pd.read_csv(path)


def _project_id_matches(proj_id_cell: str, project_id: str) -> bool:
    """Return True if project_id appears in a comma-separated ProjectIdBSV cell."""
    if pd.isna(proj_id_cell):
        return False
    return project_id in [p.strip() for p in str(proj_id_cell).split(",")]


def _build_column_index(
    mapping_df: pd.DataFrame,
    supplier: str,
    project_id: str,
) -> dict:
    """
    Pre-index the mapping CSV for fast per-column lookup.

    Matches rows by Dataleverancier == supplier AND ProjectIdBSV containing project_id.
    If project_id is None, matches all rows for the supplier regardless of project.

    Returns {variabele: {"meenemen": float|None, "bsv": str|None}}
    for all rows matching (supplier, project_id).
    Multiple rows for the same Variabele are not expected; the last one wins.
    """
    index = {}
    for _, row in mapping_df.iterrows():
        if row.get("Dataleverancier") != supplier:
            continue
        if project_id is not None:
            if not _project_id_matches(row.get("ProjectIdBSV", ""), project_id):
                continue

        variabele = str(row.get("Variabele", "")).strip()
        if not variabele:
            continue

        raw_meenemen = row.get("Mapping_Meenemen", None)
        if pd.isna(raw_meenemen) or str(raw_meenemen).strip() == "":
            meenemen = None
        else:
            try:
                meenemen = float(raw_meenemen)
            except (ValueError, TypeError):
                meenemen = None

        bsv = row.get("Mapping_VariabeleBSV", None)
        bsv = None if (pd.isna(bsv) or str(bsv).strip() == "") else str(bsv).strip()

        index[variabele] = {"meenemen": meenemen, "bsv": bsv}

    return index


def validate_mapping_coverage(
    csv_path,
    supplier: str,
    project_id: str,
    raw_columns,
    skip_columns=None,
) -> list:
    """
    Check that every raw column has a valid Mapping_Meenemen entry in the CSV.

    Parameters
    ----------
    csv_path : str or Path
        Path to the ETD DatamodelLeverancier CSV.
    supplier : str
        Supplier name to filter on (e.g. 'O-Nexus', 'Watch-E', 'FactoryZero').
    project_id : str or None
        BSV project ID string to match against the comma-split ProjectIdBSV column
        (e.g. '8', '9', '10'). Pass None to validate against all supplier rows.
    raw_columns : iterable of str
        Column names as they appear in the supplier data file.
    skip_columns : set of str, optional
        Columns to skip entirely (e.g. timestamp columns, household ID columns).
        These are not checked and not reported.

    Returns
    -------
    list of str
        Human-readable error messages, one per problematic column.
        Empty list means all columns are covered correctly.
    """
    skip = set(skip_columns) if skip_columns else set()
    mapping_df = _load_mapping_df(csv_path)
    col_index = _build_column_index(mapping_df, supplier, project_id)

    errors = []
    project_label = f"ProjectIdBSV={project_id!r}" if project_id is not None else "all projects"

    for col in raw_columns:
        if col in skip:
            logging.debug(
                f"[validate_mapping_coverage] '{col}' in skip_columns — skipping."
            )
            continue

        if col not in col_index:
            errors.append(
                f"Column '{col}' ({project_label}): not found in mapping CSV for "
                f"supplier '{supplier}'. Add a row to DatamodelLeverancier in Grist "
                f"with Mapping_Meenemen=1 (include) or Mapping_Meenemen=0 (exclude)."
            )
            logging.debug(
                f"[validate_mapping_coverage] '{col}' not in CSV for "
                f"supplier='{supplier}', {project_label}"
            )
            continue

        entry = col_index[col]
        meenemen = entry["meenemen"]
        bsv = entry["bsv"]

        if meenemen is None:
            errors.append(
                f"Column '{col}' ({project_label}): Mapping_Meenemen is empty. "
                f"Set to 1 (include with BSV target) or 0 (explicitly exclude)."
            )
        elif meenemen == 1.0:
            if bsv is None:
                errors.append(
                    f"Column '{col}' ({project_label}): Mapping_Meenemen=1 but "
                    f"Mapping_VariabeleBSV is empty. Add the BSV target column name."
                )
            else:
                logging.debug(
                    f"[validate_mapping_coverage] '{col}' -> '{bsv}' (finalized). OK."
                )
        elif meenemen == 0.0:
            logging.debug(
                f"[validate_mapping_coverage] '{col}' explicitly excluded "
                f"(Mapping_Meenemen=0). OK."
            )
        else:
            errors.append(
                f"Column '{col}' ({project_label}): Mapping_Meenemen={meenemen} "
                f"is not a valid value. Use 1 (include) or 0 (exclude)."
            )

    if errors:
        logging.warning(
            f"[validate_mapping_coverage] {len(errors)} coverage issue(s) for "
            f"supplier='{supplier}', {project_label}."
        )
    else:
        logging.info(
            f"[validate_mapping_coverage] All columns covered for "
            f"supplier='{supplier}', {project_label}. OK."
        )

    return errors


def _load_project_table(project_table_csv) -> pd.DataFrame:
    """Load the Grist Project table CSV (downloaded by sync_data_model.py)."""
    path = Path(project_table_csv)
    if not path.exists():
        raise FileNotFoundError(
            f"[supplier_validators] Project table CSV not found: {path}. "
            f"Run sync_data_model.py to download it."
        )
    return pd.read_csv(path)


def _build_site_to_project_id_map(project_df: pd.DataFrame, supplier: str = None) -> dict:
    """
    Build a {site_name: bsv_project_id_str} map from the Project table.

    The Project table from Grist has:
      - ProjectIdBSV: the BSV numeric project ID
      - ProjectIdLeverancier: the supplier's own site/project identifier (raw data value)
      - Dataleverancier: the supplier name

    The ProjectIdLeverancier column must contain the exact values that appear in
    the supplier's raw data project column (e.g. the 'Site' column in O-Nexus data).
    If these don't match, update the Grist Project table and re-run sync_data_model.py.
    """
    id_col = "ProjectIdBSV"
    name_col = "ProjectIdLeverancier"
    supplier_col = "Dataleverancier"

    if id_col not in project_df.columns or name_col not in project_df.columns:
        logging.warning(
            f"[supplier_validators] Project table columns: {list(project_df.columns)}. "
            f"Expected '{id_col}' and '{name_col}'. Returning empty site map."
        )
        return {}

    # Filter by supplier if specified
    if supplier is not None and supplier_col in project_df.columns:
        df = project_df[project_df[supplier_col] == supplier]
    else:
        df = project_df

    site_map = {}
    for _, row in df.iterrows():
        proj_id = row.get(id_col)
        site_name = row.get(name_col)
        if pd.notna(proj_id) and pd.notna(site_name) and str(site_name).strip():
            site_map[str(site_name).strip()] = str(int(float(proj_id)))

    logging.debug(f"[supplier_validators] Site->ProjectIdBSV map: {site_map}")
    return site_map


def run_mapping_coverage_preflight(
    csv_path,
    supplier: str,
    project_files: dict,
    skip_columns=None,
    raw_project_col: str = None,
    project_table_csv=None,
) -> None:
    """
    Standard pre-flight mapping coverage check for a supplier's project files.

    Call this once before starting the per-household processing loop. Reads
    a sample file from each project folder to discover raw column names and
    the site/project identifier, then validates coverage against the mapping CSV
    via validate_mapping_coverage() using BSV project IDs.

    Raises ValueError listing all issues if any are found.
    Fix issues in the DatamodelLeverancier table in Grist, re-run
    sync_data_model.py, then retry.

    Parameters
    ----------
    csv_path : str or Path
        Path to the ETD DatamodelLeverancier CSV.
    supplier : str
        Supplier name (e.g. 'O-Nexus', 'Watch-E', 'FactoryZero').
    project_files : dict[str, list[Path]]
        {project_folder_name: [parquet_path, ...]} — as collected before the
        processing loop. Each parquet file is read to discover its columns;
        only unique column names are checked.
    skip_columns : set of str, optional
        Columns to skip (e.g. timestamp candidates, household/project ID columns).
    raw_project_col : str, optional
        Name of the raw column that contains the site/project identifier
        (e.g. 'Site' for O-Nexus). When provided together with project_table_csv,
        enables per-project validation by mapping site values to BSV project IDs.
        When None, validates all columns globally against the supplier (no project filter).
    project_table_csv : str or Path, optional
        Path to the Project table CSV downloaded from Grist by sync_data_model.py
        (e.g. 'data/ETD Data model-Project.csv'). Required when raw_project_col
        is set.
    """
    import pandas as _pd

    all_errors: list[str] = []

    # Build site→ProjectIdBSV map if project-level validation is requested
    site_to_project_id: dict = {}
    if raw_project_col is not None and project_table_csv is not None:
        project_df = _load_project_table(project_table_csv)
        site_to_project_id = _build_site_to_project_id_map(project_df, supplier=supplier)
        if not site_to_project_id:
            logging.warning(
                f"[run_mapping_coverage_preflight] Site->ProjectIdBSV map is empty — "
                f"check Project table CSV at {project_table_csv}. "
                f"Falling back to supplier-level validation."
            )

    if raw_project_col is not None and site_to_project_id:
        # Per-project validation: determine the BSV project ID for each folder
        # by reading the raw_project_col value from one file in that folder,
        # then map it to a BSV project ID via the Project table.
        for project_folder, paths in project_files.items():
            # Determine the project ID for this folder from the first readable file
            folder_project_id = None
            for path in paths:
                try:
                    sample = _pd.read_parquet(path, columns=[raw_project_col])
                    if not sample.empty:
                        site_value = str(sample[raw_project_col].iloc[0]).strip()
                        folder_project_id = site_to_project_id.get(site_value)
                        if folder_project_id is None:
                            logging.warning(
                                f"[run_mapping_coverage_preflight] Site value "
                                f"'{site_value}' from '{path}' not found in Project "
                                f"table. Known sites: {list(site_to_project_id.keys())}. "
                                f"Skipping per-project check for folder '{project_folder}'."
                            )
                        break
                except Exception as e:
                    logging.warning(
                        f"[run_mapping_coverage_preflight] Could not read {path}: {e} — trying next."
                    )

            if folder_project_id is None:
                raise RuntimeError(
                    f"[run_mapping_coverage_preflight] Could not determine ProjectIdBSV "
                    f"for folder '{project_folder}' — no file in the folder returned a "
                    f"recognisable site value. Known sites: {list(site_to_project_id.keys())}. "
                    f"Update ProjectIdLeverancier in the Grist Project table to match the "
                    f"exact values in the '{raw_project_col}' column of the raw parquet files, "
                    f"then re-run sync_data_model.py."
                )

            # Collect all unique columns in this project folder
            project_cols: set[str] = set()
            for path in paths:
                raw_df = _pd.read_parquet(path, columns=None)
                project_cols.update(raw_df.columns)

            logging.info(
                f"[run_mapping_coverage_preflight] Folder '{project_folder}' -> "
                f"ProjectIdBSV={folder_project_id!r}"
            )
            errors = validate_mapping_coverage(
                csv_path=csv_path,
                supplier=supplier,
                project_id=folder_project_id,
                raw_columns=sorted(project_cols),
                skip_columns=skip_columns,
            )
            all_errors.extend(errors)
    else:
        # Supplier-level validation: collect all unique columns across all projects
        all_cols: set[str] = set()
        for paths in project_files.values():
            for path in paths:
                raw_df = _pd.read_parquet(path, columns=None)
                all_cols.update(raw_df.columns)

        all_errors = validate_mapping_coverage(
            csv_path=csv_path,
            supplier=supplier,
            project_id=None,
            raw_columns=sorted(all_cols),
            skip_columns=skip_columns,
        )

    if all_errors:
        lines = "\n  ".join(all_errors)
        msg = (
            f"[run_mapping_coverage_preflight] {len(all_errors)} coverage issue(s) found "
            f"for supplier '{supplier}'.\n"
            f"Fix these in DatamodelLeverancier in Grist and re-run sync_data_model.py.\n"
            f"Issues:\n  {lines}"
        )
        logging.error(msg)
        raise ValueError(msg)

    logging.info(
        f"[run_mapping_coverage_preflight] All projects OK for supplier '{supplier}'."
    )
