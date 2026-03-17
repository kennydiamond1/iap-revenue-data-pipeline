"""
currency_utils.py
=================
Utilities for normalising monetary amounts from IAP provider reports
into standard major-unit decimal values.

Background
----------
Payment providers (Stripe, Adyen, direct-billing APIs) submit amounts
as integers in the currency's *minor unit* (e.g. cents, fils, pence).
The number of decimal places varies by ISO 4217 currency:

  * 3-decimal currencies  → divide by 1,000  (e.g. KWD 1500 = 1.500 KWD)
  * 0-decimal currencies  → no division       (e.g. JPY 500 = 500 JPY)
  * 2-decimal currencies  → divide by 100     (default; e.g. USD 1099 = 10.99 USD)

Usage
-----
    from currency_utils import normalise_amount, get_currency_exponent

    normalise_amount(1500, "KWD")  # → Decimal("1.500")
    normalise_amount(1099, "USD")  # → Decimal("10.99")
    normalise_amount(500,  "JPY")  # → Decimal("500")
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Union

# ---------------------------------------------------------------------------
# ISO 4217 exponent tables
# ---------------------------------------------------------------------------

# 3-decimal currencies (minor unit = 1/1000)
_THREE_DECIMAL: frozenset[str] = frozenset({
    "BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND",
})

# 0-decimal currencies (minor unit = 1)
_ZERO_DECIMAL: frozenset[str] = frozenset({
    "BIF", "CLP", "CVE", "DJF", "GNF", "ISK", "JPY",
    "KMF", "KRW", "PYG", "RWF", "UGX", "VND", "VUV",
    "XAF", "XOF", "XPF",
})


def get_currency_exponent(currency_code: str) -> int:
    """
    Return the ISO 4217 minor-unit exponent for *currency_code*.

    Returns
    -------
    int
        0 → no decimal places
        2 → 2 decimal places  (default)
        3 → 3 decimal places
    """
    code = currency_code.upper().strip()
    if code in _THREE_DECIMAL:
        return 3
    if code in _ZERO_DECIMAL:
        return 0
    return 2  # default: 2 decimal places


def normalise_amount(
    minor_unit_value: Union[int, float, str, Decimal],
    currency_code: str,
) -> Decimal:
    """
    Convert a provider-submitted minor-unit integer to a major-unit Decimal.

    Parameters
    ----------
    minor_unit_value : int | float | str | Decimal
        Raw amount as received from the payment provider (e.g. 1099 for $10.99).
    currency_code : str
        ISO 4217 three-letter currency code (e.g. "USD", "KWD", "JPY").

    Returns
    -------
    Decimal
        Amount in major units, rounded to the correct number of decimal places.

    Examples
    --------
    >>> normalise_amount(1099, "USD")
    Decimal('10.99')
    >>> normalise_amount(1500, "KWD")
    Decimal('1.500')
    >>> normalise_amount(500, "JPY")
    Decimal('500')
    >>> normalise_amount("0", "GBP")
    Decimal('0.00')
    """
    value = Decimal(str(minor_unit_value))
    exponent = get_currency_exponent(currency_code)
    divisor = Decimal(10 ** exponent)
    result = value / divisor
    # Round to the currency's natural precision
    quantize_str = Decimal("0." + "0" * exponent) if exponent > 0 else Decimal("1")
    return result.quantize(quantize_str, rounding=ROUND_HALF_UP)


def format_amount(amount: Decimal, currency_code: str) -> str:
    """
    Format a major-unit Decimal with the correct number of decimal places.

    >>> format_amount(Decimal("10.9"), "USD")
    '10.90'
    >>> format_amount(Decimal("1.5"), "KWD")
    '1.500'
    >>> format_amount(Decimal("500"), "JPY")
    '500'
    """
    exponent = get_currency_exponent(currency_code)
    fmt = f".{exponent}f"
    return format(float(amount), fmt)


def is_zero_decimal(currency_code: str) -> bool:
    """Return True if *currency_code* has no minor units (e.g. JPY)."""
    return get_currency_exponent(currency_code) == 0


def is_three_decimal(currency_code: str) -> bool:
    """Return True if *currency_code* uses 3 decimal places (e.g. KWD)."""
    return get_currency_exponent(currency_code) == 3


# ---------------------------------------------------------------------------
# Batch normalisation (useful for DataFrame transformations)
# ---------------------------------------------------------------------------

def normalise_amounts_series(amounts, currencies) -> list[Decimal]:
    """
    Vectorised wrapper for DataFrame column pairs.

    Parameters
    ----------
    amounts    : iterable of numeric values (minor units)
    currencies : iterable of ISO 4217 codes, same length as amounts

    Returns
    -------
    list[Decimal]
        Major-unit values.

    Example (pandas)
    ----------------
    >>> import pandas as pd
    >>> df["amount_normalised"] = normalise_amounts_series(
    ...     df["amount_raw"], df["currency"]
    ... )
    """
    return [
        normalise_amount(amt, ccy)
        for amt, ccy in zip(amounts, currencies)
    ]


# ---------------------------------------------------------------------------
# Simple self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_cases = [
        (1099,  "USD",  "10.99"),
        (1500,  "KWD",  "1.500"),
        (500,   "JPY",  "500"),
        (0,     "GBP",  "0.00"),
        (9999,  "OMR",  "9.999"),
        (10000, "CLP",  "10000"),
        (100,   "EUR",  "1.00"),
    ]
    all_ok = True
    for raw, ccy, expected in test_cases:
        result = str(normalise_amount(raw, ccy))
        status = "✓" if result == expected else "✗"
        if result != expected:
            all_ok = False
        print(f"  {status}  normalise_amount({raw:>6}, {ccy})  →  {result:>10}  (expected {expected})")

    print("\nAll tests passed." if all_ok else "\nSome tests FAILED.")
