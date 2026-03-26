"""
IRR (XIRR) computation  v1.0.0
Implements Newton-Raphson iteration to solve for the discount rate
that makes NPV of all cashflows equal to zero.

Cashflow convention:
  Principal deposits  → NEGATIVE (money going out from investor)
  Distributions       → POSITIVE (money returned to investor)
  Terminal value      → POSITIVE (current market value of holdings)
"""
from datetime import date
from typing import List, Optional


def compute_irr(
    principal_cashflows: List[dict],
    distributions: List[dict],
    current_market_value: float,
    today: date,
    guess: float = 0.1,
    max_iterations: int = 1000,
    tolerance: float = 1e-7,
) -> Optional[float]:
    """
    Compute annualised IRR via Newton-Raphson.

    Parameters
    ----------
    principal_cashflows : list of dicts with keys 'date', 'amount', 'cashflow_type'
        subscriptions are negative cashflows, redemptions are positive
    distributions : list of dicts with keys 'date', 'amount'
        dividends/distributions received — positive cashflows
    current_market_value : float
        units × current NTA — used as terminal positive cashflow at today
    today : date
        terminal value date
    guess : float
        initial IRR guess (default 10%)

    Returns
    -------
    float or None
        Annualised IRR, or None if it could not converge
    """
    # Build unified cashflow list: (days_from_first, amount)
    all_cfs = []

    for cf in principal_cashflows:
        cf_date = cf["date"] if isinstance(cf["date"], date) else cf["date"].date()
        amount  = float(cf["amount"])
        # Subscriptions = money in from investor = negative CF in NPV convention
        if cf.get("cashflow_type") == "subscription":
            amount = -abs(amount)
        elif cf.get("cashflow_type") == "redemption":
            amount = abs(amount)
        all_cfs.append((cf_date, amount))

    for dist in distributions:
        dist_date = dist["date"] if isinstance(dist["date"], date) else dist["date"].date()
        all_cfs.append((dist_date, abs(float(dist["amount"]))))

    # Terminal value: current market value at today
    if current_market_value > 0:
        all_cfs.append((today, current_market_value))

    if not all_cfs:
        return None

    # Sort by date, compute days from earliest cashflow
    all_cfs.sort(key=lambda x: x[0])
    t0 = all_cfs[0][0]
    cashflows = [(cf[0], cf[1], (cf[0] - t0).days / 365.0) for cf in all_cfs]

    def npv(r: float) -> float:
        """Net present value at rate r"""
        if r <= -1:
            return float("inf")
        return sum(cf / ((1 + r) ** t) for _, cf, t in cashflows)

    def dnpv(r: float) -> float:
        """Derivative of NPV with respect to r"""
        if r <= -1:
            return float("inf")
        return sum(-t * cf / ((1 + r) ** (t + 1)) for _, cf, t in cashflows)

    # Newton-Raphson iteration
    r = guess
    for _ in range(max_iterations):
        f  = npv(r)
        df = dnpv(r)
        if abs(df) < 1e-12:
            break
        r_new = r - f / df
        if abs(r_new - r) < tolerance:
            return r_new
        r = r_new

    # Fallback: try different initial guesses if first fails
    for alt_guess in [-0.5, 0.0, 0.5, 1.0, 2.0]:
        r = alt_guess
        for _ in range(max_iterations):
            f  = npv(r)
            df = dnpv(r)
            if abs(df) < 1e-12:
                break
            r_new = r - f / df
            if abs(r_new - r) < tolerance:
                if -1 < r_new < 100:  # sanity check
                    return r_new
            r = r_new

    return None  # Could not converge
