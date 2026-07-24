from collections import namedtuple

Option = namedtuple("Option", "key default_value doc validator callback")


class Options:
    """Provide attribute-style access to configuration dict."""

    def __init__(self, options):
        super().__setattr__("_options", options)
        # populate with default values
        config = {}
        for key, option in options.items():
            config[key] = option.default_value

        super().__setattr__("_config", config)

    def __setattr__(self, key, value):
        # you can't set new keys
        if key in self._config:
            self._config[key] = value
        else:
            msg = f"You can only set the value of existing options, \
                {key} is not an option"
            raise AttributeError(msg)

    def __getattr__(self, key):
        # You get a clearer error message when you try
        # to get a non-existing option.
        try:
            return self._config[key]
        except KeyError:
            raise AttributeError(f"No such option: {key}") from KeyError

    def __dir__(self):
        # see list of all available options
        return list(self._config.keys())

# Define allowed Options
mapped_folder_path = Option(
    key="mapped_folder_path",
    default_value=None,
    doc=(
        "The folder containing the mapped data files"
    ),
    validator=None,
    callback=None,
)

aggregate_folder_path = Option(
    key="aggregate_folder_path",
    default_value=None,
    doc=(
        "The folder containing the data files derived from mapped data file, "
        "including those aggregated and resampled."
    ),
    validator=None,
    callback=None,
)


bsv_metadata_file = Option(
    key="bsv_metadata_file",
    default_value=None,
    doc=(
        "The file where metadata about each household is kept and managed to consistently map data source provider households to BSV households"

    ),
    validator=None,
    callback=None,
)

etdmodel_csv_path = Option(
    key="etdmodel_csv_path",
    default_value=None,
    doc=(
        "Override path to the ETD model CSV file. When None (default), the bundled "
        "etdmap/data/etdmodel.csv is used. Set this to use your own data model CSV, "
        "e.g. etdmap.options.etdmodel_csv_path = '/path/to/my_model.csv'."
    ),
    validator=None,
    callback=None,
)

supplier_mapping_csv_path = Option(
    key="supplier_mapping_csv_path",
    default_value=None,
    doc=(
        "Path to the ETD DatamodelLeverancier CSV (synced from the source "
        "data model). Maps raw "
        "supplier column names to BSV column names and drives "
        "load_supplier_pipeline_config / map_raw_df in all supplier mappers."
    ),
    validator=None,
    callback=None,
)

project_mapping_csv_path = Option(
    key="project_mapping_csv_path",
    default_value=None,
    doc=(
        "Path to the CSV that maps ProjectIdLeverancier values to ProjectIdBSV. "
        "Used to resolve project identifiers for suppliers whose "
        "households are not yet in the BSV metadata file at the time of mapping."
    ),
    validator=None,
    callback=None,
)

household_batch_csv_path = Option(
    key="household_batch_csv_path",
    default_value=None,
    doc=(
        "Path to the synced HuisBatch table CSV: one row per household batch "
        "with HuisIdBSV, HuisBatchIdBSV, BatchIdBSV, Meenemen, "
        "Gegevensfrequentie, Leverancierfrequentie, Startdatum and Einddatum. "
        "The project's data-management tooling produces this file; "
        "save_index_to_parquet reads it to populate the household-batch "
        "columns of index.parquet. When unset or missing, those columns are "
        "omitted with a warning."
    ),
    validator=None,
    callback=None,
)

mapped_output_format = Option(
    key="mapped_output_format",
    default_value="sharded",
    doc=(
        "How run_standard_pipeline writes mapped household data. 'sharded' "
        "(the one-way default): hive-partitioned shards under "
        "<mapped_folder_path>/sharded/HuisIdBSV=<n>/HuisBatchIdBSV=<p>/. "
        "'flat': the legacy single household_<n>_table.parquet. Producing "
        "flat output is a deliberate per-run choice (mapper scripts expose a "
        "--flat flag; tests pin the value); it is NOT a maintained "
        "configuration -- do not set it in config files."
    ),
    validator=None,
    callback=None,
)

# Set the option with default values
options = Options(
    {
        "mapped_folder_path": mapped_folder_path,
        "aggregate_folder_path": aggregate_folder_path,
        "bsv_metadata_file": bsv_metadata_file,
        "etdmodel_csv_path": etdmodel_csv_path,
        "supplier_mapping_csv_path": supplier_mapping_csv_path,
        "project_mapping_csv_path": project_mapping_csv_path,
        "household_batch_csv_path": household_batch_csv_path,
        "mapped_output_format": mapped_output_format,
    }
)

# We use a more extended version of this simple example:
# The upsides: you cannot set (or mistype) options that don't exist -
# this will generate an error, and you can see a list of the
# options available as well as their documentation.

# class Options:
#     def __init__(self):
#         self.mapped_folder = None
#         self.aggregate_folder = None

# options = Options()


