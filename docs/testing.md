# Test development in VSCode (configuration)
We have developed tests mostly using VSCode. These files can be added to the .vscode folder to setup the test environment.

Example of settings.json to be able to run individual tests (and generate the test data fixture) in the VSCode debug tab for test development
``` json
{
    "python.testing.pytestArgs": [
        "tests"
    ],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

Example of launch.json to be able to run individual tests (and generate the test data fixture) in the VSCode debug tab for test development
``` json
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