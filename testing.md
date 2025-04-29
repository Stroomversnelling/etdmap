# Test development in VSCode (configuration)

Tests should be run when making changes to the `etdmap` package. In addition, downstream `etdtransform` tests should also be run.

We have developed tests mostly using VSCode. These files can be added to the `.vscode` folder to setup the test environment.

Example of `settings.json` to be able to run individual tests (and generate the test data fixture) in the VSCode debug tab for test development:

```json
{
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}

```

Example of `launch.json` to be able to run individual tests (and generate the test data fixture) in the VSCode debug tab for test development:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug pytest",
            "type": "debugpy",
            "request": "launch",
            "module": "pytest",
            "args": [
            ],
            "console": "integratedTerminal"
        }
    ]
}


```

One can also use the following to copy the test data out. This is no longer really necessary

```json

{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug pytest with copy-data",
            "type": "debugpy",
            "request": "launch",
            "module": "pytest",
            "args": [
                "tests/test_raw_data.py::test_raw_data_fixture",
                "--copy-data"
            ],
            "console": "integratedTerminal"
        }
    ]
}

```

# Updating the data model between versions

Use the script in `etdworkflow`, `update_data_model.py` that provides detailed feedback on changes in the Excel based data model in comparison to the installed `etdmap` version in the environment with suggestions of variables to add and remove in `etdmap`. 

In addition, it is critical to run the etdmap pytests in the debugger to be sure everything is working.