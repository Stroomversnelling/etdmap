"""Bitwise flags recorded during mapping, and the checks that set them.

A flag says what was found about a value, one bit per finding, so a value can
carry several at once. Zero means nothing was found: the value is validated.

Flags are set once, where the finding is made. Later passes filter on a bit
rather than repeating the test that produced it.
"""

from enum import IntFlag, auto
from math import inf

import pandas as pd


class MappingFlag(IntFlag):
    """
    What is wrong with a single value, one bit per kind of fault.

    Each bit names a class of error, independent of which pass happens to
    detect it or what any pass decides to do about it.
    """

    NONE = 0

    # The value is outside the range its variable is allowed to take. Too
    # small or too large to be a real measurement of this quantity.
    OUT_OF_BOUNDS_LOW = auto()
    OUT_OF_BOUNDS_HIGH = auto()

    # A meter total that went backwards, and then returned to at least where it
    # had been. The meter never lost count, so the readings across the dip are
    # the faulty ones.
    CUMULATIVE_JUMP_BACK = auto()

    # A meter total that went backwards more than once in succession. The meter
    # is unreliable across that stretch rather than glitching on one reading.
    CUMULATIVE_REPEAT_NEGATIVE = auto()

    # A meter total that went backwards and then counted up again from the
    # lower value. The register restarted -- a replacement, a rollover or a
    # reset -- so consumption cannot be derived across the restart.
    CUMULATIVE_METER_RESET = auto()

    # A meter total that went backwards and never rose again. The meter stopped
    # producing usable totals, so nothing from that point on can be trusted.
    CUMULATIVE_NO_RECOVERY = auto()

    # A meter total reading exactly zero after it had already been counting.
    # A register that has accumulated cannot return to zero, so the device is
    # reporting an absent reading as the number zero.
    CUMULATIVE_ZERO_TOTAL = auto()

    # A register that stopped advancing while the device kept consuming. The
    # consumption is not lost, it is late: it arrives in one lump when the
    # register reports again, so these readings understate and one overstates.
    FROZEN_SPAN = auto()

    # The reading a frozen span's accumulation arrives in. Too large to be one
    # interval's consumption, and paired with the span it belongs to.
    CATCHUP = auto()

    # Faults whose bad readings span from the decrease to the recovery, rather
    # than sitting on a single reading.
    CUMULATIVE_DECREASE_RANGE = CUMULATIVE_JUMP_BACK | CUMULATIVE_REPEAT_NEGATIVE


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
    lower_bound : float or pd.NA, optional
        Min from the thresholds table. pd.NA means no lower bound is set.
    upper_bound : float or pd.NA, optional
        Max from the thresholds table. pd.NA means no upper bound is set.

    Returns
    -------
    pd.Series
        Int64 of ``MappingFlag`` bits. MappingFlag.NONE (zero) means the value
        is within every bound that was set.
    """
    present = series.notna()
    low = (series < (-inf if pd.isna(lower_bound) else lower_bound)) & present
    high = (series > (inf if pd.isna(upper_bound) else upper_bound)) & present

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

    A bound is written against a unit of time. ``5-minute`` bounds say what one
    five-minute interval may hold, so a register that reports hourly delivers
    twelve intervals' worth in one reading and must be judged against twelve
    times the bound. Judging it against the five-minute figure would call every
    normal hourly reading impossible.

    **Only ``5-minute`` bounds scale.** A ``momentaan`` bound is an instantaneous
    reading -- power does not accumulate over a longer window, so a slower
    reporting rate does not make a larger value plausible. A ``cumulatief`` bound
    is a yearly total, a different grain entirely. Both are returned unchanged.

    Returns pd.NA unchanged: a bound that is not set stays not set.
    """
    if pd.isna(bound) or pd.isna(cadence_minutes):
        return bound
    if threshold_type != "5-minute":
        return bound
    return float(bound) * (float(cadence_minutes) / THRESHOLD_BASE_MINUTES)


def contiguous_runs(mask: pd.Series) -> list[tuple[int, int]]:
    """
    The (start, stop) positions of each maximal run of True in *mask*.

    Positions are half-open, so ``stop`` is the first position after the run --
    which is also the position of whatever ended it.

    pd.NA is not True. A mask carrying it would otherwise read as a run, and an
    unknown is not a finding.
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
    Flag registers that stopped advancing and then caught up in one lump.

    A flat stretch is not a fault by itself -- a device that is not running does
    not consume, and a summer of no space heating looks exactly like a broken
    sensor until you see what follows. What makes it a fault is the **catch-up**:
    a reading too large to be one interval's consumption, sitting where the
    register started reporting again. That reading is the accumulation arriving
    late.

    So detection is anchored on the catch-up and walks backwards. A flat stretch
    with nothing impossible after it is left alone.

    *intervals* is a per-interval series -- a Diff column, not a register.
    A missing interval counts as "did not advance": a gap in transmission still
    means no consumption was recorded, and a span must not be split in two by
    one absent reading.

    A **negative** interval is not part of a span. A register that goes backwards
    is a reset, which ``CUMULATIVE_METER_RESET`` already names, and swallowing it
    here would give one fault two names and extend a span across the reset.

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

    The detection ``frozen_span_flags`` renders. See that function for what makes
    a flat stretch a fault rather than an idle device.

    Returns
    -------
    pd.DataFrame
        ``start`` and ``catchup_position`` are positions into *intervals*;
        the span covers ``start`` up to but not including ``catchup_position``.
        ``n_intervals`` is how long the register stood still, ``catchup_value``
        is what arrived, and ``upper_bound`` is what made it impossible.
    """
    rows = []
    if not (pd.isna(upper_bound) or intervals.empty):
        # A missing interval counts as "did not advance". A negative one does
        # not: testing for zero exactly excludes it, which a decrease never is.
        did_not_advance = intervals.fillna(0) == 0
        catch_up = (intervals > upper_bound).fillna(False)

        # A run's stop position is the reading that ended it. When that reading
        # is a catch-up, the run before it is the span it belongs to.
        span_ending_at = {
            stop: start for start, stop in contiguous_runs(did_not_advance)
        }

        # Walk backwards from the impossible readings only. There are few of
        # them; starting from the flat runs would visit every quiet stretch in
        # the series to discover almost none of them end in one.
        by_position = catch_up.reset_index(drop=True)
        for position in by_position[by_position].index:
            start = span_ending_at.get(int(position))
            if start is None:
                # An impossible reading with no flat run before it is a lone
                # spike, which the bound check already reports. Not a span.
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
