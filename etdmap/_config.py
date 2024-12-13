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
    key="mapping_folder_path",
    default_value=None,
    doc=(
        "The folder containing the raw data files"
    ),
    validator=None,
    callback=None,
)

bsv_metadata_file = Option(
    key="bsv_metadata_file",
    default_value=None,
    doc=(
        "BSV_METADATA_FILE is the file where metadata about each household is kept and managed to consistently map data source provider households to BSV households"

    ),
    validator=None,
    callback=None,
)

# Set the option with default values
options = Options(
    {
        "mapped_folder_path": mapped_folder_path,
        "bsv_metadata_file": bsv_metadata_file
    }
)

# We use a more extended version of this simple example:
# The upsides: you cannot set (or mistype) options that don't exist -
# this will generate an error, and you can see a list of the
# options available as well as their documentation.

# class Options:
#     def __init__(self):
#         self.mapping_folder = None
#         self.aggregate_folder = None

# options = Options()


