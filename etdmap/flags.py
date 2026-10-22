"""Findings recorded against individual values during mapping.

Each finding is one bit of a :class:`MappingFlag`, so a value may carry several
at once. Zero means no finding applies.

Flags are set where the finding is made and read downstream, so a later stage
filters on a bit rather than repeating the test that produced it.
"""

import re
from enum import IntFlag, auto
from math import inf

import pandas as pd


class MappingFlag(IntFlag):
    """Findings that may apply to a single value.

    Each bit names a class of fault independently of which stage detects it or
    what any stage does in response.
    """

    NONE = 0

    # --- signals: something observed about the value ---

    # Outside the range declared for the variable.
    OUT_OF_BOUNDS_LOW = auto()
    OUT_OF_BOUNDS_HIGH = auto()

    # A cumulative variable decreased and then returned to at least its previous
    # value. The device did not lose count, so the readings across the decrease
    # are the faulty ones.
    CUMULATIVE_JUMP_BACK = auto()

    # A cumulative variable decreased more than once in succession, indicating an
    # unreliable stretch rather than a single faulty reading.
    CUMULATIVE_REPEAT_NEGATIVE = auto()

    # A cumulative variable decreased and then counted up from the lower value: a
    # replacement, rollover or reset. Consumption cannot be derived across it.
    CUMULATIVE_METER_RESET = auto()

    # A cumulative variable decreased and never increased again. No usable total
    # is produced from that point onward.
    CUMULATIVE_NO_RECOVERY = auto()

    # A cumulative variable read zero after it had accumulated. A total cannot
    # return to zero, so the device is reporting an absent reading as zero.
    CUMULATIVE_ZERO_TOTAL = auto()

    # A cumulative variable did not advance while consumption continued. The
    # consumption is recorded later, in the reading flagged CATCHUP.
    FROZEN_SPAN = auto()

    # The reading in which a frozen span's accumulated consumption arrives.
    # Larger than one interval may hold, and paired with that span.
    CATCHUP = auto()

    # --- actions: something the pipeline did to the value ---

    # Carried forward from an earlier reading because the device reports
    # infrequently. No measurement was taken at this timestamp.
    FILLED_DOWN = auto()

    # Set to zero because the device is absent for this household. No
    # measurement was taken; a zero set here is not a reported zero.
    FILLED_ZERO = auto()

    # The zero seeded at a household's first interval. Differencing has no
    # predecessor there, so the value is structural rather than measured.
    FIRST_ZERO = auto()

    # Blanked to pd.NA. Records that a value was removed, which the signal that
    # condemned it does not say on its own.
    NULLED = auto()

    # The cumulative column was built from the delivered interval column
    # because the supplier delivered only the interval form.
    CUMULATIVE_CONSTRUCTED = auto()

    # Mapping examined the value and wrote nothing. Distinguishes a value that
    # was left alone from one no stage reached.
    NOT_CHANGED = auto()

    # The tariff root was written as the sum of its delivered Hoog/Laag
    # registers. Like CUMULATIVE_CONSTRUCTED, the value carries what the
    # delivered registers reported, so it is neither INVENTED nor excluded
    # from measurement.
    TARIFF_COMBINED = auto()

    # Both tariff registers delivered a nonzero value at the same reading.
    # Peak and off-peak registers alternate, so simultaneous consumption on
    # both is a suspicious delivered pattern. Observation only.
    TARIFF_SIMULTANEOUS = auto()

    # The supplier delivered a value in this cell. Set before any stage blanks
    # a value, so a delivered-then-removed cell carries this and NULLED.
    RAW_DELIVERED_VALUE = auto()

    # pd.NA on a row the column was never due to report. The column reports
    # every k grid rows and this row is not one of them, so nothing is absent.
    OFF_CADENCE_NA = auto()

    # pd.NA on a row where the column was due to report. A reading the column
    # owed is absent, which is what OFF_CADENCE_NA is not.
    MISSING_VALUE = auto()

    # The target was written by combining several delivered source columns.
    # Like TARIFF_COMBINED, the value carries what the delivered sources
    # reported, so it is neither INVENTED nor excluded from measurement. Kept
    # separate from TARIFF_COMBINED so a tariff sum stays distinguishable from
    # a sensor mean.
    COMBINED_COLUMNS = auto()

    # The supplier sent something the column's declared type could not read, so
    # the coercion removed it. The condition behind the NULLED on that cell.
    UNPARSEABLE_VALUE = auto()

    # Some of a combined target's source columns delivered at this reading and
    # some did not. The combination preserves pd.NA, so the target holds nothing
    # and the delivered sources are dropped with it.
    SOURCES_INCOMPLETE = auto()

    # More than one reading fell in this grid slot. One was kept and the rest
    # were discarded, so this value was selected rather than the only candidate.
    SLOT_COLLISION = auto()

    # The interval was derived by differencing the delivered cumulative column,
    # replacing whatever the supplier sent for the interval itself. Like
    # TARIFF_COMBINED, the value carries what the supplier reported, so it is
    # neither INVENTED nor excluded from measurement.
    INTERVAL_FROM_DELIVERED_CUMULATIVE = auto()

    # The interval is negative on a variable that cannot decrease. Monotonicity
    # is declared on the cumulative variable, so it is derivable for the
    # interval paired with it; elsewhere a decrease is simply the reading, and
    # this is never set there. Independent of the threshold pass: that only
    # reports a negative where the interval column happens to declare a Min.
    NEGATIVE_INTERVAL = auto()

    # A frozen span on a column holding the supplier's own interval amounts,
    # resolved by spreading the catch-up evenly over the span it covers. The
    # catch-up's total is measured; its distribution across the span is not, so
    # each written value is invented even though the column's total is not.
    FROZEN_DELIVERED_INTERVAL_SPREAD = auto()

    # The grid holds a slot here and the source frame delivered.
    SLOT_NOT_DELIVERED = auto()

    # The two categories, declared rather than left to a comment. A signal names
    # something observed about a value; an action names something the pipeline
    # did to it. Every single-bit member belongs to exactly one, which a test
    # pins, so a report can label any flag without keeping its own list.
    SIGNALS = (
        OUT_OF_BOUNDS_LOW
        | OUT_OF_BOUNDS_HIGH
        | CUMULATIVE_JUMP_BACK
        | CUMULATIVE_REPEAT_NEGATIVE
        | CUMULATIVE_METER_RESET
        | CUMULATIVE_NO_RECOVERY
        | CUMULATIVE_ZERO_TOTAL
        | FROZEN_SPAN
        | CATCHUP
        | TARIFF_SIMULTANEOUS
        | RAW_DELIVERED_VALUE
        | OFF_CADENCE_NA
        | MISSING_VALUE
        | UNPARSEABLE_VALUE
        | SOURCES_INCOMPLETE
        | SLOT_COLLISION
        | NEGATIVE_INTERVAL
        | SLOT_NOT_DELIVERED
    )
    ACTIONS = (
        FILLED_DOWN
        | FILLED_ZERO
        | FIRST_ZERO
        | NULLED
        | CUMULATIVE_CONSTRUCTED
        | NOT_CHANGED
        | TARIFF_COMBINED
        | COMBINED_COLUMNS
        | INTERVAL_FROM_DELIVERED_CUMULATIVE
        | FROZEN_DELIVERED_INTERVAL_SPREAD
    )

    # Faults whose affected readings run from the decrease to the recovery
    # rather than occupying a single reading.
    CUMULATIVE_DECREASE_RANGE = CUMULATIVE_JUMP_BACK | CUMULATIVE_REPEAT_NEGATIVE

    # Values written or removed by the pipeline rather than measured. Actions
    # only: a signal here would exclude values that were merely observed.
    # CUMULATIVE_CONSTRUCTED is deliberately absent -- the column was built, but
    # its values carry what the delivered intervals reported.
    INVENTED = (
        FILLED_DOWN
        | FILLED_ZERO
        | NULLED
        | FIRST_ZERO
        | FROZEN_DELIVERED_INTERVAL_SPREAD
    )

    # Every action the pipeline may take on a value, except recording that it
    # took none. NOT_CHANGED is set where this is absent, so the two are
    # defined from one list rather than maintained separately.
    # TARIFF_COMBINED, COMBINED_COLUMNS and INTERVAL_FROM_DELIVERED_CUMULATIVE
    # are here (an action was taken) but, like CUMULATIVE_CONSTRUCTED, not in
    # INVENTED: the written value carries what the supplier reported.
    ACTIONS_TAKEN = (
        INVENTED
        | CUMULATIVE_CONSTRUCTED
        | TARIFF_COMBINED
        | COMBINED_COLUMNS
        | INTERVAL_FROM_DELIVERED_CUMULATIVE
    )

    # A cumulative fault that destroys the count. No consumption can be derived
    # across these readings, whatever values they carry.
    CUMULATIVE_UNUSABLE = (
        CUMULATIVE_JUMP_BACK
        | CUMULATIVE_REPEAT_NEGATIVE
        | CUMULATIVE_METER_RESET
        | CUMULATIVE_NO_RECOVERY
        | CUMULATIVE_ZERO_TOTAL
    )

    # Not a measurement of this variable in this interval: written rather than
    # read, read for a different interval, condemned by a cumulative fault, or
    # outside what the variable may hold. The one test a project average filters
    # on. Any future finding that makes a value unusable as evidence belongs
    # here.
    # SLOT_COLLISION is deliberately absent: the kept reading is a delivered
    # measurement, one candidate among several, and excluding it would discard
    # real data from a project average.
    NOT_A_MEASUREMENT = (
        INVENTED
        | FROZEN_SPAN
        | CATCHUP
        | CUMULATIVE_UNUSABLE
        | OUT_OF_BOUNDS_LOW
        | OUT_OF_BOUNDS_HIGH
        | UNPARSEABLE_VALUE
        | SOURCES_INCOMPLETE
        | NEGATIVE_INTERVAL
    )


#: Suffix marking a column as carrying findings rather than data. Defined once
#: so the naming rule and any scan over flag columns cannot drift apart.
FLAG_SUFFIX = "_flags"


def flag_column(column: str) -> str:
    """The name of the column carrying findings about *column*."""
    return f"{column}{FLAG_SUFFIX}"


def set_flags(df, column, mask, flag) -> None:
    """
    Record a finding about *column* on the rows *mask* selects.

    Findings accumulate: a cell can be both filled down and out of bounds, so
    the bit is OR-ed in rather than assigned. The flag column is created on first
    use, which keeps callers from having to initialise it.

    Modifies *df* in place.
    """
    name = flag_column(column)
    if name not in df.columns:
        df[name] = pd.Series(
            MappingFlag.NONE.value, index=df.index, dtype="Int64"
        )

    mask = mask.fillna(False) if hasattr(mask, "fillna") else mask
    if not bool(mask.any()):
        return

    # A missing history is not a value to interpret, it is a broken invariant.
    # This column is created as NONE on every row and only ever has bits OR-ed
    # in, so pd.NA can only arrive from a step that added rows to a frame
    # already carrying it without initialising them. Refusing matters because
    # `pd.NA | flag` is pd.NA: the write below would be swallowed in silence,
    # leaving no error and no record.
    #
    # Scoped to the rows being written, not the whole column, so the cost is
    # proportional to the mask rather than the frame -- this runs on every
    # flag write. Completeness is not owed here:
    # :func:`assert_flag_history_complete` scans every column once at the end
    # of the pipeline, which is what catches damage no later write touches. The
    # check here is to localise the failure to the write that would have lost
    # the bit.
    target = df.loc[mask, name]
    if bool(target.isna().any()):
        missing = int(target.isna().sum())
        raise ValueError(
            f"{name} holds no recorded history on {missing} of the "
            f"{int(mask.sum())} row(s) being flagged, so OR-ing {flag.name} in "
            f"would be discarded (pd.NA | flag is pd.NA). A flag column is "
            f"created as NONE on every row and only accumulates bits, so this "
            f"means a step added rows without initialising them -- initialise "
            f"the new rows to MappingFlag.NONE where they are added."
        )

    df.loc[mask, name] = target | flag.value


def mark_undelivered_slots(df) -> None:
    """Record ``SLOT_NOT_DELIVERED`` on the flag cells a completed grid left bare.

    For the steps that bring a frame onto its full set of slots -- padding a
    household onto its grid, merging source frames that cover different
    periods. Those slots arrive with pd.NA in every column no row delivered
    into them, flag columns included.

    Recording the signal rather than writing ``NONE`` is the point. ``NONE``
    says nothing happened here; what happened is that the grid has a slot and
    the source delivered nothing into it, which is a fact about the cell and
    says why its value is absent. Setting a bit also replaces the pd.NA, so
    recording the signal is what restores the invariant that makes every LATER
    finding on those cells recordable -- ``pd.NA | flag`` is pd.NA, so without
    this the next write is discarded in silence.

    Call it where the grid is completed, not before a write. By the time
    :func:`set_flags` runs the cell must already carry a history; pd.NA there
    means a step skipped this, and is refused rather than repaired.

    Assigns directly rather than going through :func:`set_flags`, which refuses
    a missing history by design. This is the one sanctioned exception, and it
    lives here beside the only other writer so that flag manipulation stays in
    one module.

    Modifies *df* in place.
    """
    for name in df.columns:
        if not str(name).endswith(FLAG_SUFFIX):
            continue
        missing = df[name].isna()
        if bool(missing.any()):
            df[name] = (
                df[name]
                .where(~missing, MappingFlag.SLOT_NOT_DELIVERED.value)
                .astype("Int64")
            )


def assert_flag_history_complete(df, context: str = "") -> None:
    """Refuse a frame where any flag column holds no recorded history.

    One scan over every ``<column>_flags`` column, reported per column. A flag
    column is created as ``NONE`` on every row and only ever accumulates bits,
    so pd.NA in one means a step added rows to a frame already carrying it
    without initialising them -- and ``pd.NA | flag`` is pd.NA, so every
    subsequent finding on those cells is discarded in silence.

    The completeness half of the guard. :func:`set_flags` refuses a write onto
    a missing history, but only sees the rows it is asked to write: a column
    that no later step writes to carries the damage to disk with nothing to
    trip over. Measured on the 2026-10-21 estate mapping, 52 flag columns held
    pd.NA and only 4 of them ever saw a later write.

    Reported per column rather than as a total because which columns are
    damaged identifies the step that did it -- on that run the damaged set was
    exactly the columns flagged before the grid padding.

    Missingness in a DATA column is untouched by this: there it is a
    first-class value, and only the flag columns carry the no-pd.NA invariant.

    Raises ``ValueError`` naming each damaged column and its count.
    """
    damaged = []
    for name in df.columns:
        if not str(name).endswith(FLAG_SUFFIX):
            continue
        missing = int(df[name].isna().sum())
        if missing:
            damaged.append((str(name), missing))

    if not damaged:
        return

    where = f"{context}: " if context else ""
    detail = ", ".join(f"{name} on {count} row(s)" for name, count in damaged)
    raise ValueError(
        f"{where}{len(damaged)} flag column(s) hold no recorded history, so "
        f"any finding written to those cells would be discarded "
        f"(pd.NA | flag is pd.NA): {detail}. Of {len(df.index)} row(s) total. "
        f"A flag column is created as NONE on every row and only accumulates "
        f"bits, so a step added rows without initialising them -- initialise "
        f"the new rows to MappingFlag.NONE where they are added."
    )


def _as_limit(bound, unbounded, index):
    """
    Return a bound in a form a comparison can use, as a scalar or one per row.

    A bound may be one number for the whole column, or one number per row: a
    cadence-scaled bound varies by household, and a constraint-derived bound
    varies by row, being a formula in that row's other cells.

    A bound that is not set is no limit, so it is replaced by *unbounded* and
    flags nothing. The same applies per row: pd.NA within a Series bound leaves
    those rows unbounded.
    """
    if isinstance(bound, pd.Series):
        return bound.reindex(index).fillna(unbounded)
    if bound is None or pd.isna(bound):
        return unbounded
    return bound


def bound_flags(
    series: pd.Series,
    lower_bound=pd.NA,
    upper_bound=pd.NA,
) -> pd.Series:
    """
    Flag the values that fall outside their bounds.

    Thresholds may be one-sided. A bound that is not set is no limit on that
    side -- the same state as a variable with no threshold row at all -- so it
    is compared against infinity and flags nothing. A missing value is flagged
    on neither side: a value that is not there cannot be out of range.

    Parameters
    ----------
    series : pd.Series
        The values to check.
    lower_bound : float, pd.Series or pd.NA, optional
        Min from the thresholds table. pd.NA means no lower bound is set. A
        Series gives a bound per row, for a bound that is not one number for the
        whole column -- scaled to a household's cadence, or derived from the
        other cells in the row.
    upper_bound : float, pd.Series or pd.NA, optional
        Max, same forms as lower_bound.

    Returns
    -------
    pd.Series
        Int64 of ``MappingFlag`` bits. MappingFlag.NONE (zero) means the value
        is within every bound that was set.
    """
    present = series.notna()
    low = (series < _as_limit(lower_bound, -inf, series.index)) & present
    high = (series > _as_limit(upper_bound, inf, series.index)) & present

    # .value throughout: a Flag member is iterable on Python 3.11+, so pandas
    # would read the enum itself as array-like.
    return (
        low.astype("Int64") * MappingFlag.OUT_OF_BOUNDS_LOW.value
        | high.astype("Int64") * MappingFlag.OUT_OF_BOUNDS_HIGH.value
    )


#: Units a ThresholdType may name its interval in, in minutes.
_THRESHOLD_INTERVAL_UNITS = {
    "second": 1 / 60,
    "minute": 1.0,
    "hour": 60.0,
}

_THRESHOLD_INTERVAL_PATTERN = re.compile(
    r"^(\d+)-(" + "|".join(_THRESHOLD_INTERVAL_UNITS) + r")s?$"
)


def threshold_base_minutes(threshold_type):
    """
    The interval a threshold's bound is written against, in minutes.

    A ThresholdType that names an interval states what one such interval may
    hold: ``5-minute`` is a bound per five minutes, and the same row written
    ``1-hour`` would be a bound per hour. The interval is read from the label so
    that the threshold row is the only place it is recorded.

    Returns pd.NA for a type that does not name an interval. ``momentaan`` is an
    instantaneous reading, which does not accumulate over a longer interval;
    ``cumulatief`` is a yearly total; ``algemeen`` is dimensionless, such as a
    percentage. None of the three scales with a reporting interval.
    """
    if threshold_type is None or pd.isna(threshold_type):
        return pd.NA

    match = _THRESHOLD_INTERVAL_PATTERN.match(str(threshold_type).strip())
    if match is None:
        return pd.NA

    minutes = int(match.group(1)) * _THRESHOLD_INTERVAL_UNITS[match.group(2)]
    return minutes if minutes > 0 else pd.NA


def scale_bound_to_cadence(bound, threshold_type, cadence_minutes):
    """
    Scale a per-interval bound to the interval the column actually reports in.

    A bound is declared against a unit of time. A ``5-minute`` bound states what
    one five-minute interval may hold, so a column reporting hourly delivers
    twelve intervals of consumption in one reading and is judged against twelve
    times the declared bound. Judging it against the five-minute figure would
    reject every normal hourly reading.

    Only a bound whose type names an interval scales -- see
    :func:`threshold_base_minutes` for which do. The rest are returned
    unchanged, as is a bound that is not set.
    """
    base_minutes = threshold_base_minutes(threshold_type)
    if pd.isna(bound) or pd.isna(cadence_minutes) or pd.isna(base_minutes):
        return bound
    return float(bound) * (float(cadence_minutes) / float(base_minutes))


def contiguous_runs(mask: pd.Series) -> list[tuple[int, int]]:
    """
    The (start, stop) positions of each maximal run of True in *mask*.

    Positions are half-open: ``stop`` is the first position after the run, which
    is also the position of the value that ended it.

    pd.NA is treated as False. An unknown is not a finding, and a mask carrying
    pd.NA would otherwise read as part of a run.
    """
    flat = mask.fillna(False).reset_index(drop=True).astype("int8")
    if not flat.any():
        return []

    edges = flat.diff()
    edges.iloc[0] = flat.iloc[0]

    starts = edges[edges == 1].index.tolist()
    ends = edges[edges == -1].index.tolist()
    if len(ends) < len(starts):
        # A run still open at the last row ends where the series does.
        ends.append(len(flat))

    return [(int(s), int(e)) for s, e in zip(starts, ends)]


def frozen_span_flags(
    intervals: pd.Series,
    upper_bound=pd.NA,
    missing_counts_as_frozen: bool = True,
) -> pd.Series:
    """
    Flag a cumulative variable that stopped advancing and then caught up.

    A flat stretch is not a fault by itself: a device that is not running does
    not consume, and a summer without space heating is indistinguishable from a
    stalled sensor. What identifies the fault is the catch-up -- a reading too
    large to be one interval of consumption, at the point where the cumulative
    variable resumed. That reading holds the accumulation that arrived late.

    Detection is therefore anchored on the catch-up and walks backwards. A flat
    stretch not followed by a catch-up is left alone.

    *intervals* is a per-interval series, that is, a Diff column rather than a
    cumulative variable. By default a missing interval counts as not advancing:
    no consumption was recorded, and a span must not be split in two by a
    single absent reading. On a column holding the supplier's own interval
    amounts that reading of an absence is wrong -- pass
    *missing_counts_as_frozen=False*, and see :func:`frozen_spans` for which
    caller owns which reading.

    A negative interval is not part of a span. A cumulative variable going
    backwards is a reset, named by ``CUMULATIVE_METER_RESET``; including it here
    would give one fault two names and extend a span across the reset.

    Parameters
    ----------
    intervals : pd.Series
        Per-interval consumption (a Diff column).
    upper_bound : float or pd.NA, optional
        The largest plausible value for one interval, from the thresholds table
        and scaled to the column's cadence. pd.NA means no bound is set, so
        nothing is impossible and nothing is flagged.

    Returns
    -------
    pd.Series
        Int64 of ``MappingFlag`` bits: FROZEN_SPAN on the readings that did not
        advance, CATCHUP on the reading their accumulation arrived in.
    """
    spans = frozen_spans(
        intervals,
        upper_bound=upper_bound,
        missing_counts_as_frozen=missing_counts_as_frozen,
    )

    frozen = pd.Series(False, index=intervals.index)
    paired = pd.Series(False, index=intervals.index)
    for span in spans.itertuples():
        frozen.iloc[span.start:span.catchup_position] = True
        paired.iloc[span.catchup_position] = True

    # .value throughout: a Flag member is iterable on Python 3.11+, so pandas
    # would read the enum itself as array-like.
    return (
        frozen.astype("Int64") * MappingFlag.FROZEN_SPAN.value
        | paired.astype("Int64") * MappingFlag.CATCHUP.value
    )


_SPAN_DTYPES = {
    "start": "Int64",
    "catchup_position": "Int64",
    "n_intervals": "Int64",
    "catchup_value": "Float64",
    "upper_bound": "Float64",
}


def frozen_spans(
    intervals: pd.Series,
    upper_bound=pd.NA,
    missing_counts_as_frozen: bool = True,
) -> pd.DataFrame:
    """
    One row per frozen span, with the catch-up its accumulation arrived in.

    This is the same detection that :func:`frozen_span_flags` renders as flags,
    reported span by span. See that function for what distinguishes a frozen
    span from an idle device.

    Parameters
    ----------
    intervals : pd.Series
        Per-interval amounts.
    upper_bound : float or pd.NA, optional
        The largest plausible value for one interval. pd.NA flags nothing.
    missing_counts_as_frozen : bool, optional
        What an absent interval means, which differs by how the column was
        produced and is not something this function can tell:

        True (default), for an interval DERIVED from a delivered cumulative
        column -- an absence means no advance was recorded, so it belongs to
        the span and must not split one in two.

        False, for a column holding the supplier's OWN interval amounts --
        there `pd.NA` is unknown and only `0.0` is a reported zero, so an
        absence ends the run rather than extending it. The comparison then
        yields `pd.NA` on absent cells and :func:`contiguous_runs` reads that
        as not part of a run.

    Returns
    -------
    pd.DataFrame
        ``start`` and ``catchup_position`` are positions into *intervals*; the
        span covers ``start`` up to but not including ``catchup_position``.
        ``n_intervals`` is how many intervals the cumulative variable stood
        still, ``catchup_value`` is the reading its accumulation arrived in, and
        ``upper_bound`` is the threshold that reading exceeded.
    """
    rows = []
    if not (pd.isna(upper_bound) or intervals.empty):
        # Testing for exactly zero excludes decreases either way: a cumulative
        # variable going backwards is a reset, named by CUMULATIVE_METER_RESET.
        if missing_counts_as_frozen:
            did_not_advance = intervals.fillna(0) == 0
        else:
            did_not_advance = intervals == 0
        catch_up = (intervals > upper_bound).fillna(False)

        # A run's stop position is the reading that ended it. When that reading
        # is a catch-up, the run before it is the span it belongs to.
        span_ending_at = {
            stop: start for start, stop in contiguous_runs(did_not_advance)
        }

        # Iterating the catch-ups rather than the flat runs: catch-ups are few,
        # while most quiet stretches are not followed by one.
        by_position = catch_up.reset_index(drop=True)
        for position in by_position[by_position].index:
            start = span_ending_at.get(int(position))
            if start is None:
                # An over-bound reading with no flat run before it is a single
                # outlier, already reported by the bound check, and not a span.
                continue
            rows.append(
                {
                    "start": start,
                    "catchup_position": int(position),
                    "n_intervals": int(position) - start,
                    "catchup_value": float(intervals.iloc[int(position)]),
                    "upper_bound": float(upper_bound),
                }
            )

    return pd.DataFrame(rows, columns=list(_SPAN_DTYPES)).astype(_SPAN_DTYPES)


#: One line per member, saying what the flag means. Kept here beside the class
#: so a new flag and its meaning land in one change. Every single-bit member
#: must appear; a test enforces it.
DESCRIPTIONS = {
    "OUT_OF_BOUNDS_LOW": "Below the range declared for the variable.",
    "OUT_OF_BOUNDS_HIGH": "Above the range declared for the variable.",
    "CUMULATIVE_JUMP_BACK": (
        "A cumulative variable decreased and then returned to at least its "
        "previous value."
    ),
    "CUMULATIVE_REPEAT_NEGATIVE": "A cumulative variable decreased more than once in succession.",
    "CUMULATIVE_METER_RESET": (
        "A cumulative variable decreased and then counted up from the lower "
        "value."
    ),
    "CUMULATIVE_NO_RECOVERY": "A cumulative variable decreased and never increased again.",
    "CUMULATIVE_ZERO_TOTAL": "A cumulative variable read zero after it had accumulated.",
    "FROZEN_SPAN": "A cumulative variable did not advance while consumption continued.",
    "CATCHUP": "The reading a frozen span's accumulated consumption arrives in.",
    "FILLED_DOWN": "A value carried forward from an earlier reading.",
    "FILLED_ZERO": (
        "DEPRECATED. Nothing sets this any more. A zero written for a device "
        "the household does not have. Device absence is declared in the "
        "household metadata and the catalog substitutes zero inside the "
        "formula, so no zero is written into the data. The member is kept so "
        "data written before that still decodes."
    ),
    "FIRST_ZERO": "The zero at a household's first interval.",
    "NULLED": "A value blanked to pd.NA.",
    "CUMULATIVE_CONSTRUCTED": (
        "DEPRECATED. Nothing sets this any more. A cumulative column built from "
        "the delivered interval column. Mapping no longer builds one -- a "
        "running total summed from intervals is not a meter reading -- and the "
        "member is kept so data written before that still decodes."
    ),
    "NOT_CHANGED": (
        "A cell that holds a value and carries no recorded action. Set on "
        "every data column, so an empty findings column means no step reached "
        "the column rather than that it left the value alone. Never set on an "
        "empty cell: see MISSING_VALUE and OFF_CADENCE_NA for those."
    ),
    "TARIFF_COMBINED": "A tariff root written as the sum of its delivered Hoog/Laag registers.",
    "TARIFF_SIMULTANEOUS": "Both tariff registers delivered nonzero at the same reading.",
    "RAW_DELIVERED_VALUE": (
        "A value the supplier delivered in this cell, recorded before any "
        "stage blanks or replaces one."
    ),
    "OFF_CADENCE_NA": (
        "Absent because the column was not due to report on this row."
    ),
    "MISSING_VALUE": (
        "Absent on a row where the column was due to report."
    ),
    "SLOT_NOT_DELIVERED": (
        "The grid holds a slot here and the source delivered no row into it."
    ),
    "COMBINED_COLUMNS": (
        "A target written by combining several delivered source columns, as a "
        "sum or a mean. The sources are dropped after the combine, so without "
        "this the target reads as delivered whole."
    ),
    "UNPARSEABLE_VALUE": (
        "The supplier sent a value the column's declared type could not read, "
        "so coercion removed it. The condition behind the NULLED on that cell."
    ),
    "SOURCES_INCOMPLETE": (
        "Some of a combined target's source columns delivered at this reading "
        "and some did not. The combination preserves pd.NA, so the target "
        "holds nothing and the delivered sources were dropped with it."
    ),
    "SLOT_COLLISION": (
        "More than one reading fell in this grid slot. One was kept and the "
        "rest discarded, so this value was selected rather than the only "
        "candidate. Still a measurement: the kept reading was delivered."
    ),
    "FROZEN_DELIVERED_INTERVAL_SPREAD": (
        "A frozen span on a column holding the supplier's own interval "
        "amounts, resolved by spreading the catch-up evenly over the span it "
        "covers. The column's total is unchanged; each written value is "
        "invented, because the catch-up's total is measured but its "
        "distribution across the span is not."
    ),
    "NEGATIVE_INTERVAL": (
        "The interval is negative on a variable that cannot decrease. "
        "Monotonicity is declared on the cumulative variable and derived for "
        "the interval paired with it, so this needs no declared threshold -- "
        "unlike OUT_OF_BOUNDS_LOW, which reports a negative only where the "
        "interval column declares a Min. Never set where a decrease is simply "
        "the reading."
    ),
    "INTERVAL_FROM_DELIVERED_CUMULATIVE": (
        "The interval was derived by differencing the delivered cumulative "
        "column. Where the supplier delivered an interval too, that delivered "
        "value was replaced; RAW_DELIVERED_VALUE beside this flag is what says "
        "so."
    ),
}


def flag_kind(flag) -> str:
    """``"signal"`` or ``"action"``, read from the flag class's own composites.

    A signal names a condition a value was found in; an action names something a
    stage did to it. Both flag classes declare the split as ``SIGNALS`` and
    ``ACTIONS`` composite members, and those are the reference -- a second list
    of which flag is which drifts from them silently, so nothing keeps one.

    Raises ``ValueError`` for a member in both or in neither, which is a flag
    that was added without being categorised.
    """
    owner = type(flag)
    is_signal = bool(flag & owner.SIGNALS)
    is_action = bool(flag & owner.ACTIONS)
    if is_signal == is_action:
        raise ValueError(
            f"{owner.__name__}.{flag.name} is in "
            f"{'both' if is_signal else 'neither'} of SIGNALS and ACTIONS. "
            f"Every single-bit member must be in exactly one."
        )
    return "signal" if is_signal else "action"
