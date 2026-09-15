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
