"""The packaged project registry resolves each project's physical model.

Written by the Grist sync beside the other data files; built into a dict at
import because rule selection reads it per row.
"""

import pandas as pd

from etdmap.data_model import load_projects, physical_model_by_project


class TestTheRegistry:
    def test_it_carries_the_three_columns(self):
        df = load_projects()
        assert list(df.columns) == [
            "Dataleverancier", "ProjectIdBSV", "FysischModel",
        ]

    def test_every_project_declares_exactly_one_model(self):
        df = load_projects()
        assert len(df), "the synced registry is empty"
        assert not df["ProjectIdBSV"].duplicated().any()
        assert not df["FysischModel"].isna().any()

    def test_the_dict_mirrors_the_file(self):
        df = load_projects()
        assert physical_model_by_project == {
            int(row.ProjectIdBSV): str(row.FysischModel)
            for row in df.itertuples()
            if pd.notna(row.ProjectIdBSV) and pd.notna(row.FysischModel)
        }
