import logging

import pandas as pd
import pytest

from etdmap import data_model
from etdmap.data_model import (
    all_performance_data_columns,
    cumulative_column,
    cumulative_columns,
    data_analysis_columns,
    diff_columns,
    get_aggregation_config,
    interval_column,
    model_column_order,
    required_performance_data_columns,
)
from etdmap.testing import cumulative_pair, model_row


def test_thresholdscsv():
    """
    Check thresholds.csv for numeric and cumulative colums:

    - Check if thresholds exist for all numeric columns in datamodel.
    - Check if all thresholds are numeric in thresholds.csv
    - Check if the cumulative types match the cumulative_columns from etdmap.data_model
    """
    etdmodel_csv = data_model.load_etdmodel()
    thresholds_csv = data_model.load_thresholds()
    numeric_cols_with_range = set(
        etdmodel_csv[
            (etdmodel_csv['Type variabele'] == 'number') &
            (etdmodel_csv['Entiteit'] == 'Prestatiedata') &
            (etdmodel_csv['Min'].notna() | etdmodel_csv['Max'].notna())
        ].Variabele
    )
    threshold_params = set(thresholds_csv.Variabele)

    # Only require threshold entries for number columns that have Min or Max defined in the model.
    # Columns without any range in the model (e.g. categorical codes) do not need threshold entries.
    assert numeric_cols_with_range.issubset(threshold_params)

    assert pd.to_numeric(thresholds_csv['Min'], errors='coerce').notna().equals(thresholds_csv['Min'].notna()), "Min has non-numeric non-missing values (text?)"
    assert pd.to_numeric(thresholds_csv['Max'], errors='coerce').notna().equals(thresholds_csv['Max'].notna()), "Max has non-numeric non-missing values (text?)"

    # Check cummulative columns:
    cumm_columns_thresholds = set(
        thresholds_csv[thresholds_csv['ThresholdType']=='cumulatief'].Variabele
        )

    # Check if all cumulative columns in the etdmap.data_model
    # are also specified in the thresholds.csv
    assert set(cumulative_columns).issubset(cumm_columns_thresholds)
    # Notify if more cumulative columns are specified in the thresholds:
    if cumm_columns_thresholds - set(cumulative_columns):
        logging.warning(
            f"More cumulative columns are defined in thresholds.csv"
            f"then in etdmap.data_model cumulative_columns. "
            f"The following columns are found, but not required: "
            f"{cumm_columns_thresholds - set(cumulative_columns)}"
            )


# ---------------------------------------------------------------------------
# get_aggregation_config
# ---------------------------------------------------------------------------

def test_get_aggregation_config_returns_known_columns():
    config = get_aggregation_config()
    assert isinstance(config, dict)
    assert len(config) > 0
    # A known Diff column must be present with the expected method values
    assert "ElektriciteitNetgebruikHoogDiff" in config
    entry = config["ElektriciteitNetgebruikHoogDiff"]
    assert entry["resample_method"] == "sum"
    assert entry["aggregate_method"] == "avg"


def test_get_aggregation_config_raises_on_missing_resample_method(tmp_path, monkeypatch):

    import etdmap

    model = data_model.load_etdmodel()
    # Clear ResamplingMethode for a known included variable
    model.loc[model["Variabele"] == "ElektriciteitNetgebruikHoogDiff", "ResamplingMethode"] = None
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    with pytest.raises(ValueError, match="ResamplingMethode"):
        get_aggregation_config()


def test_get_aggregation_config_raises_on_missing_aggregate_method(tmp_path, monkeypatch):

    import etdmap

    model = data_model.load_etdmodel()
    model.loc[model["Variabele"] == "ElektriciteitNetgebruikHoogDiff", "AggregatieMethode"] = None
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    with pytest.raises(ValueError, match="AggregatieMethode"):
        get_aggregation_config()


def test_get_aggregation_config_raises_on_inconsistent_derive(tmp_path, monkeypatch):
    """A 'derive' method on only one axis is a modelling error (ADR-003)."""

    import etdmap

    model = data_model.load_etdmodel()
    # Normalise any pre-existing 'derive' variable in the real model so this
    # test controls the single induced inconsistency below.
    both = (model["ResamplingMethode"] == "derive") | (model["AggregatieMethode"] == "derive")
    model.loc[both, ["ResamplingMethode", "AggregatieMethode"]] = "derive"
    # 'derive' on Resampling but not Aggregatie for a known included variable.
    mask = model["Variabele"] == "ElektriciteitNetgebruikHoogDiff"
    model.loc[mask, "ResamplingMethode"] = "derive"
    model.loc[mask, "AggregatieMethode"] = "avg"
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    with pytest.raises(ValueError, match="inconsistent 'derive'"):
        get_aggregation_config()


def test_get_aggregation_config_accepts_consistent_derive(tmp_path, monkeypatch):
    """'derive' on BOTH axes is valid and passes through to the config."""

    import etdmap

    model = data_model.load_etdmodel()
    # Normalise any pre-existing half-applied 'derive' in the real model so the
    # whole config is consistent, then add another consistent 'derive' variable.
    both = (model["ResamplingMethode"] == "derive") | (model["AggregatieMethode"] == "derive")
    model.loc[both, ["ResamplingMethode", "AggregatieMethode"]] = "derive"
    mask = model["Variabele"] == "ElektriciteitNetgebruikHoogDiff"
    model.loc[mask, "ResamplingMethode"] = "derive"
    model.loc[mask, "AggregatieMethode"] = "derive"
    patched = tmp_path / "etdmodel_patched.csv"
    model.to_csv(patched, index=False)

    monkeypatch.setattr(etdmap.options, "etdmodel_csv_path", str(patched))
    config = get_aggregation_config()
    entry = config["ElektriciteitNetgebruikHoogDiff"]
    assert entry["resample_method"] == "derive"
    assert entry["aggregate_method"] == "derive"


# ---------------------------------------------------------------------------
# What each derived list is, stated as behaviour (etdmap/DECISIONS.md ADR-001).
#
# Each test declares a row with the property in question and reads the list
# back. Deriving the expected answer from the shipped model a second time, here,
# would pass whenever the two derivations agree -- including when both are
# wrong -- and say nothing about what the rule is.
# ---------------------------------------------------------------------------

@pytest.fixture
def model_restored():
    """Restores the shipped model, whatever the test declared."""
    yield
    data_model.rebuild_model()


class TestWhatCumulativeColumnsIs:
    def test_it_is_the_cumulatief_flag(self, model_restored):
        rows, thresholds = cumulative_pair("QCumulative")
        data_model.rebuild_model(
            extra_model_rows=rows, extra_threshold_rows=thresholds
        )

        assert "QCumulative" in cumulative_columns

    def test_the_entiteit_does_not_narrow_it(self, model_restored):
        """ADR-001 item 1: the flag alone, with no Entiteit filter. A
        calculated variable that accumulates is still a cumulative one."""
        rows, thresholds = cumulative_pair(
            "QCalculated", Entiteit="PrestatiedataBerekend"
        )
        data_model.rebuild_model(
            extra_model_rows=rows, extra_threshold_rows=thresholds
        )

        assert "QCalculated" in cumulative_columns

    def test_a_date_typed_row_is_excluded(self, model_restored):
        """ReadingDate carries Cumulatief=ja in some versions of the model.
        Filtering on the type rather than the name is what keeps it out."""
        data_model.rebuild_model(extra_model_rows=[
            model_row("QDate", Cumulatief="ja", **{"Type variabele": "date"})
        ])

        assert "QDate" not in cumulative_columns

    def test_a_row_without_the_flag_is_not_in_it(self, model_restored):
        data_model.rebuild_model(extra_model_rows=[model_row("QPlain")])

        assert "QPlain" not in cumulative_columns


class TestWhatModelColumnOrderIs:
    def test_it_holds_prestatiedata_rows(self, model_restored):
        data_model.rebuild_model(extra_model_rows=[model_row("QDelivered")])

        assert "QDelivered" in model_column_order

    def test_a_calculated_row_is_not_in_it(self, model_restored):
        """Its callers -- mapping_helpers, dataset_validators -- expect only
        what a supplier may deliver."""
        data_model.rebuild_model(extra_model_rows=[model_row("QCalculated", Entiteit="PrestatiedataBerekend")])

        assert "QCalculated" not in model_column_order
        assert "QCalculated" in all_performance_data_columns

    def test_volgorde_decides_the_order(self, model_restored):
        data_model.rebuild_model(extra_model_rows=[
            model_row("QLate", Volgorde=99998),
            model_row("QEarly", Volgorde=-1),
        ])

        assert model_column_order.index("QEarly") < model_column_order.index("QLate")

    def test_the_first_declared_position_comes_first(self, model_restored):
        """A row with the lowest Volgorde in the model sorts ahead of every
        shipped column, so the ordering is the declared sequence and not the
        order rows happen to sit in."""
        data_model.rebuild_model(extra_model_rows=[model_row("QFirst", Volgorde=-1)])

        assert model_column_order[0] == "QFirst"

    def test_data_analysis_columns_is_the_same_list(self, model_restored):
        """An alias, not a copy: a declaration reaches both names."""
        data_model.rebuild_model(extra_model_rows=[model_row("QDelivered")])

        assert data_analysis_columns == model_column_order


class TestWhatRequiredPerformanceDataColumnsIs:
    def test_a_required_row_is_in_it(self, model_restored):
        data_model.rebuild_model(extra_model_rows=[model_row("QRequired", Vereist="ja")])

        assert "QRequired" in required_performance_data_columns

    def test_an_optional_row_is_not(self, model_restored):
        data_model.rebuild_model(extra_model_rows=[model_row("QOptional", Vereist="nee")])

        assert "QOptional" not in required_performance_data_columns
        assert "QOptional" in all_performance_data_columns

    def test_a_required_calculated_row_is_in_it_too(self, model_restored):
        """Every Prestatiedata* entity, not the delivered one alone."""
        data_model.rebuild_model(extra_model_rows=[
            model_row("QRequiredCalc", Vereist="ja", Entiteit="PrestatiedataBerekend")
        ])

        assert "QRequiredCalc" in required_performance_data_columns

    def test_the_required_are_a_subset_of_all(self):
        assert set(required_performance_data_columns) <= set(
            all_performance_data_columns
        )

    def test_all_includes_everything_in_the_order(self):
        assert set(model_column_order) <= set(all_performance_data_columns)


class TestCumulativeIntervalPairs:
    """The link between a cumulative variable and its interval column.

    ``ComplementVariabele`` states it. Until that column is synced the link is
    read off the names, which cannot express every pair, so these pin what each
    source is allowed to mean.
    """

    def test_every_cumulative_variable_has_an_interval_column(self):
        for column in cumulative_columns:
            assert interval_column(column) is not None

    def test_the_link_goes_both_ways(self):
        for column in cumulative_columns:
            assert cumulative_column(interval_column(column)) == column

    def test_a_non_column_is_not_paired(self):
        assert interval_column("ReadingDate") is None
        assert cumulative_column("ReadingDate") is None

    def test_a_name_is_not_a_relationship(self):
        """Nothing is paired by its name. A column named <x>Diff is the interval
        column of <x> only because the model says so."""
        for column in diff_columns:
            if not column.endswith("Diff"):
                continue
            stem = column[: -len("Diff")]
            if cumulative_column(column) is None:
                continue
            # Where they do match, it is the declared link that says so.
            assert interval_column(cumulative_column(column)) == column
            assert stem in cumulative_columns or cumulative_column(column) != stem


class TestTheShippedModelIsConsistent:
    """The bundled etdmodel.csv, checked as data.

    ComplementVariabele is maintained in Grist and synced here, so an error in
    it arrives as a file change. etdmap reads the committed model as given, so
    this is where a broken one is caught -- alongside the same checks run
    against Grist by sync_data_model before it writes.
    """

    @pytest.fixture(scope="class")
    def model(self):
        return data_model.load_etdmodel()

    @staticmethod
    def _declared(model):
        if "ComplementVariabele" not in model.columns:
            return {}
        return {
            str(row["Variabele"]): str(row["ComplementVariabele"]).strip()
            for _, row in model.iterrows()
            if pd.notna(row.get("Variabele"))
            and pd.notna(row.get("ComplementVariabele"))
            and str(row["ComplementVariabele"]).strip() != ""
        }

    def test_every_complement_names_a_variable_that_exists(self, model):
        variables = set(model["Variabele"].dropna().astype(str))
        unknown = {
            variable: complement
            for variable, complement in self._declared(model).items()
            if complement not in variables
        }
        assert not unknown, f"ComplementVariabele names unknown variables: {unknown}"

    def test_no_variable_is_its_own_complement(self, model):
        declared = self._declared(model)
        assert not [v for v, c in declared.items() if v == c]

    def test_each_pair_is_one_cumulative_and_one_interval_variable(self, model):
        cumulative = set(cumulative_columns)
        intervals = set(diff_columns)
        wrong = [
            f"{variable} <-> {complement}"
            for variable, complement in self._declared(model).items()
            if not (
                (variable in cumulative and complement in intervals)
                or (variable in intervals and complement in cumulative)
            )
        ]
        assert not wrong, (
            f"a complement pair must be one Cumulatief=ja variable and one "
            f"Diff=ja variable: {wrong}"
        )

    def test_the_complement_link_is_one_to_one(self, model):
        """An interval column belongs to exactly one cumulative variable, and a
        cumulative variable has exactly one interval column.

        One to one is what makes "the interval column of this variable" a
        well-defined thing to write and "the cumulative variable behind this
        column" a well-defined thing to read.
        """
        cumulative = set(cumulative_columns)
        intervals_of, cumulative_of = {}, {}
        for variable, complement in self._declared(model).items():
            if variable in cumulative:
                pair = (variable, complement)
            elif complement in cumulative:
                pair = (complement, variable)
            else:
                continue
            intervals_of.setdefault(pair[0], set()).add(pair[1])
            cumulative_of.setdefault(pair[1], set()).add(pair[0])

        several_intervals = {
            c: sorted(i) for c, i in intervals_of.items() if len(i) > 1
        }
        several_owners = {
            i: sorted(c) for i, c in cumulative_of.items() if len(c) > 1
        }
        assert not several_intervals, (
            f"cumulative variables with more than one interval column: "
            f"{several_intervals}"
        )
        assert not several_owners, (
            f"interval columns claimed by several cumulative variables: "
            f"{several_owners}"
        )

    def test_both_sides_agree_where_both_are_filled(self, model):
        declared = self._declared(model)
        disagreeing = {
            variable: (complement, declared[complement])
            for variable, complement in declared.items()
            if complement in declared and declared[complement] != variable
        }
        assert not disagreeing, f"ComplementVariabele sides disagree: {disagreeing}"

    def test_every_cumulative_variable_declares_its_interval_column(self, model):
        """There is no fallback to a name convention. A cumulative variable with
        no declared complement has no interval column at all, and mapping would
        derive no interval values for it.

        Declaring it on either row is enough; the relation is symmetric.
        """
        declared = self._declared(model)
        stated = set(declared) | set(declared.values())
        missing = sorted(set(cumulative_columns) - stated)
        assert not missing, (
            f"cumulative variables with no ComplementVariabele on either row: "
            f"{missing}"
        )

    def test_interval_columns_of_one_variable_share_a_threshold(self, model):
        """Two interval columns linked to the same cumulative variable are the
        same quantity under two names. Whichever one the pipeline writes is
        judged the same way, so renaming a variable cannot quietly change what
        counts as plausible."""
        thresholds = {
            str(row["Variabele"]): (row.get("Min"), row.get("Max"),
                                    str(row.get("ThresholdType", "")).strip())
            for _, row in model.iterrows()
            if pd.notna(row.get("Variabele"))
        }
        cumulative = set(cumulative_columns)

        owned = {}
        for variable, complement in self._declared(model).items():
            if variable in cumulative:
                owned.setdefault(variable, set()).add(complement)
            elif complement in cumulative:
                owned.setdefault(complement, set()).add(variable)

        def same(a, b):
            return all(
                (pd.isna(x) and pd.isna(y)) or x == y for x, y in zip(a, b)
            )

        disagreeing = {}
        for owner, intervals in owned.items():
            declared = {i: thresholds.get(i) for i in sorted(intervals)}
            first = next(iter(declared.values()), None)
            if first and not all(same(first, other) for other in declared.values()):
                disagreeing[owner] = declared

        assert not disagreeing, (
            f"interval columns of one cumulative variable declare different "
            f"thresholds: {disagreeing}"
        )

    def test_an_interval_column_is_not_signed_when_its_variable_cannot_be(
        self, model
    ):
        """A cumulative variable that never decreases produces interval values
        that are never negative, so a negative Min on its interval column admits
        readings the quantity cannot produce.

        Only pairs the model links are checked. A net quantity with no
        cumulative complement is legitimately signed.
        """
        cumulative = set(cumulative_columns)
        minimum = {
            str(row["Variabele"]): pd.to_numeric(row.get("Min"), errors="coerce")
            for _, row in model.iterrows()
            if pd.notna(row.get("Variabele"))
        }

        pairs = set()
        for variable, complement in self._declared(model).items():
            if variable in cumulative:
                pairs.add((variable, complement))
            elif complement in cumulative:
                pairs.add((complement, variable))

        signed = {
            interval: (minimum.get(interval), owner, minimum.get(owner))
            for owner, interval in sorted(pairs)
            if pd.notna(minimum.get(owner)) and minimum[owner] >= 0
            and pd.notna(minimum.get(interval)) and minimum[interval] < 0
        }
        assert not signed, (
            f"interval columns allow a negative value their cumulative variable "
            f"cannot produce: {signed}"
        )


class TestComplementVariabele:
    """Reading the link.

    The model is committed data and is read as given; what makes it trustworthy
    is the sync check and the tests against the shipped file above.
    """

    @staticmethod
    def _pairs(rows, cumulative, intervals):
        return data_model._build_cumulative_interval_pairs(
            pd.DataFrame(rows), cumulative=cumulative, intervals=intervals
        )

    def test_it_pairs_names_that_share_no_stem(self):
        """What the metadata is for: no rule over the names can pair
        ZonopwekBruto with ZonopwekTotaal."""
        forward, backward = self._pairs(
            [
                {"Variabele": "ZonopwekTotaal", "ComplementVariabele": "ZonopwekBruto"},
                {"Variabele": "ZonopwekBruto", "ComplementVariabele": "ZonopwekTotaal"},
            ],
            ["ZonopwekTotaal"], ["ZonopwekBruto"],
        )
        assert forward == {"ZonopwekTotaal": "ZonopwekBruto"}
        assert backward == {"ZonopwekBruto": "ZonopwekTotaal"}

    @pytest.mark.parametrize(
        "declared_on", ["the cumulative row", "the interval row", "both rows"]
    )
    def test_the_side_it_is_written_on_does_not_matter(self, declared_on):
        """The relation is one to one, so either row states it in both
        directions. Reading only one side would leave a variable whose
        complement is declared on the other row with no interval column, and
        mapping would then derive no interval values for it.
        """
        rows = [
            {"Variabele": "ZonopwekTotaal", "ComplementVariabele": None},
            {"Variabele": "ZonopwekBruto", "ComplementVariabele": None},
        ]
        if declared_on in ("the cumulative row", "both rows"):
            rows[0]["ComplementVariabele"] = "ZonopwekBruto"
        if declared_on in ("the interval row", "both rows"):
            rows[1]["ComplementVariabele"] = "ZonopwekTotaal"

        forward, backward = self._pairs(
            rows, ["ZonopwekTotaal"], ["ZonopwekBruto"]
        )
        assert forward == {"ZonopwekTotaal": "ZonopwekBruto"}
        assert backward == {"ZonopwekBruto": "ZonopwekTotaal"}

    def test_nothing_is_paired_without_the_column(self):
        """There is no fallback. A model that does not state the link states no
        pairs, rather than pairs guessed from the names."""
        assert self._pairs(
            [{"Variabele": "ZonopwekTotaal"}, {"Variabele": "ZonopwekTotaalDiff"}],
            ["ZonopwekTotaal"], ["ZonopwekTotaalDiff"],
        ) == ({}, {})

    def test_a_name_that_looks_like_a_pair_is_not_one(self):
        """<x> and <x>Diff are paired because the model says so, not because
        they share a stem."""
        forward, _ = self._pairs(
            [
                {"Variabele": "Gasgebruik", "ComplementVariabele": None},
                {"Variabele": "GasgebruikDiff", "ComplementVariabele": None},
            ],
            ["Gasgebruik"], ["GasgebruikDiff"],
        )
        assert forward == {}

    def test_a_pair_that_is_not_one_of_each_kind_raises(self):
        """The link is then unreadable, not merely wrong."""
        with pytest.raises(ValueError, match="not one Cumulatief"):
            self._pairs(
                [
                    {"Variabele": "A", "ComplementVariabele": "B"},
                    {"Variabele": "B", "ComplementVariabele": "A"},
                ],
                ["A"], ["C"],
            )


if __name__ == "__main__":
    # Run pytest for debugging the testing
    pytest.main(["-v"])

