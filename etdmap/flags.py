"""Findings recorded against individual values during mapping.

Each finding is one bit of a :class:`MappingFlag`, so a value may carry several
at once. Zero means no finding applies.

Flags are set where the finding is made and read downstream, so a later stage
filters on a bit rather than repeating the test that produced it.
"""

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
    )
    ACTIONS = (
        FILLED_DOWN
        | FILLED_ZERO
        | FIRST_ZERO
        | NULLED
        | CUMULATIVE_CONSTRUCTED
        | NOT_CHANGED
    )

    # Faults whose affected readings run from the decrease to the recovery
    # rather than occupying a single reading.
    CUMULATIVE_DECREASE_RANGE = CUMULATIVE_JUMP_BACK | CUMULATIVE_REPEAT_NEGATIVE

    # Values written or removed by the pipeline rather than measured. Actions
    # only: a signal here would exclude values that were merely observed.
    # CUMULATIVE_CONSTRUCTED is deliberately absent -- the column was built, but
    # its values carry what the delivered intervals reported.
    INVENTED = FILLED_DOWN | FILLED_ZERO | NULLED | FIRST_ZERO

    # Every action the pipeline may take on a value, except recording that it
    # took none. NOT_CHANGED is set where this is absent, so the two are
    # defined from one list rather than maintained separately.
    ACTIONS_TAKEN = INVENTED | CUMULATIVE_CONSTRUCTED

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
    NOT_A_MEASUREMENT = (
        INVENTED
        | FROZEN_SPAN
        | CATCHUP
        | CUMULATIVE_UNUSABLE
        | OUT_OF_BOUNDS_LOW
        | OUT_OF_BOUNDS_HIGH
    )


def flag_column(column: str) -> str:
    """The name of the column carrying findings about *column*."""
    return f"{column}_flags"


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
    df.loc[mask, name] = df.loc[mask, name] | flag.value


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


# The interval a "5-minute" threshold is written against.
THRESHOLD_BASE_MINUTES = 5


def scale_bound_to_cadence(bound, threshold_type, cadence_minutes):
    """
    Scale a per-interval bound to the interval the column actually reports in.

    A bound is declared against a unit of time. A ``5-minute`` bound states what
    one five-minute interval may hold, so a column reporting hourly delivers
    twelve intervals of consumption in one reading and is judged against twelve
    times the declared bound. Judging it against the five-minute figure would
    reject every normal hourly reading.

    Only ``5-minute`` bounds scale. A ``momentaan`` bound applies to an
    instantaneous reading, which does not accumulate over a longer interval, so
    a slower reporting rate does not make a larger value plausible. A
    ``cumulatief`` bound applies to a yearly total. Both are returned unchanged,
    as is a bound that is not set.
    """
    if pd.isna(bound) or pd.isna(cadence_minutes):
        return bound
    if threshold_type != "5-minute":
        return bound
    return float(bound) * (float(cadence_minutes) / THRESHOLD_BASE_MINUTES)


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
    cumulative variable. A missing interval counts as not advancing: no
    consumption was recorded, and a span must not be split in two by a single
    absent reading.

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
    spans = frozen_spans(intervals, upper_bound=upper_bound)

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
) -> pd.DataFrame:
    """
    One row per frozen span, with the catch-up its accumulation arrived in.

    This is the same detection that :func:`frozen_span_flags` renders as flags,
    reported span by span. See that function for what distinguishes a frozen
    span from an idle device.

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
        # A missing interval counts as not advancing. A negative one does not:
        # testing for exactly zero excludes decreases.
        did_not_advance = intervals.fillna(0) == 0
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
    "FILLED_ZERO": "A zero written for a device the household does not have.",
    "FIRST_ZERO": "The zero at a household's first interval.",
    "NULLED": "A value blanked to pd.NA.",
    "CUMULATIVE_CONSTRUCTED": "A cumulative column built from the delivered interval column.",
    "NOT_CHANGED": "A value mapping examined and left as delivered.",
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
