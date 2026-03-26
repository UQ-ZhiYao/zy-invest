"""
Price fetcher service  v1.0.0
Auto-fetches daily closing prices via yahooquery.
Falls back gracefully — marks instruments as needing manual input.
"""
import asyncio
from datetime import date
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)


async def fetch_prices_yahoo(tickers: List[str]) -> Dict[str, float]:
    """
    Fetch latest closing prices for a list of Yahoo Finance tickers.
    Returns dict of {ticker: price}. Missing tickers are omitted.
    """
    if not tickers:
        return {}

    try:
        # Run yahooquery in thread pool (it's synchronous)
        loop = asyncio.get_event_loop()
        prices = await loop.run_in_executor(None, _yahoo_fetch_sync, tickers)
        return prices
    except Exception as e:
        logger.error(f"Yahoo price fetch failed: {e}")
        return {}


def _yahoo_fetch_sync(tickers: List[str]) -> Dict[str, float]:
    """Synchronous yahooquery fetch — run in executor"""
    try:
        from yahooquery import Ticker
        t = Ticker(tickers, timeout=15)
        quotes = t.price

        prices = {}
        for ticker, data in quotes.items():
            if isinstance(data, dict) and "regularMarketPrice" in data:
                price = data["regularMarketPrice"]
                if price and price > 0:
                    prices[ticker] = float(price)
            # Some tickers return string errors — skip those
        return prices
    except ImportError:
        logger.warning("yahooquery not installed — run: pip install yahooquery")
        return {}
    except Exception as e:
        logger.error(f"yahooquery sync fetch error: {e}")
        return {}


async def run_daily_price_fetch(db) -> dict:
    """
    Main daily price fetch routine.
    Called by scheduler at 6:00 PM MYT after Bursa closes.
    Returns summary of fetch results.
    """
    today = date.today()

    # Get all active instruments with Yahoo tickers
    instruments = await db.fetch(
        """
        SELECT tm.instrument, tm.yahoo_ticker, tm.is_manual
        FROM ticker_map tm
        JOIN holdings h ON h.instrument = tm.instrument
        WHERE h.units > 0
        """
    )

    auto_tickers  = []
    manual_needed = []
    ticker_to_instr = {}

    for row in instruments:
        if not row["is_manual"] and row["yahoo_ticker"]:
            auto_tickers.append(row["yahoo_ticker"])
            ticker_to_instr[row["yahoo_ticker"]] = row["instrument"]
        else:
            manual_needed.append(row["instrument"])

    # Fetch auto prices
    fetched   = await fetch_prices_yahoo(auto_tickers)
    succeeded = []
    failed    = []

    for yahoo_ticker, price in fetched.items():
        instrument = ticker_to_instr.get(yahoo_ticker)
        if not instrument:
            continue
        try:
            # Upsert into price_history
            await db.execute(
                """
                INSERT INTO price_history (instrument, date, price, source)
                VALUES ($1, $2, $3, 'yahoo')
                ON CONFLICT (instrument, date) DO UPDATE SET price = EXCLUDED.price
                """,
                instrument, today, price
            )
            # Update ticker_map last_price
            await db.execute(
                """
                UPDATE ticker_map SET last_price = $1, last_price_date = $2, updated_at = NOW()
                WHERE instrument = $3
                """,
                price, today, instrument
            )
            # Update holdings market value
            await db.execute(
                """
                UPDATE holdings
                SET last_price    = $1,
                    market_value  = units * $1,
                    unrealized_pl = (units * $1) - total_costs,
                    return_pct    = CASE WHEN total_costs > 0
                                    THEN ((units * $1) - total_costs) / total_costs
                                    ELSE 0 END,
                    updated_at    = NOW()
                WHERE instrument = $2
                """,
                price, instrument
            )
            succeeded.append(instrument)
        except Exception as e:
            logger.error(f"Failed to save price for {instrument}: {e}")
            failed.append(instrument)

    # Mark auto-tickers that weren't returned as stale
    for yahoo_ticker in auto_tickers:
        if yahoo_ticker not in fetched:
            instrument = ticker_to_instr.get(yahoo_ticker)
            if instrument:
                failed.append(instrument)

    return {
        "date":           str(today),
        "succeeded":      succeeded,
        "failed":         failed,
        "manual_needed":  manual_needed,
        "total_fetched":  len(succeeded),
        "needs_attention": len(failed) + len(manual_needed),
    }


async def update_manual_price(db, instrument: str, price: float, admin_user_id: str):
    """Admin manually sets price for OTC/warrant instruments"""
    today = date.today()
    await db.execute(
        """
        INSERT INTO price_history (instrument, date, price, source)
        VALUES ($1, $2, $3, 'admin_manual')
        ON CONFLICT (instrument, date) DO UPDATE
        SET price = EXCLUDED.price, source = 'admin_manual'
        """,
        instrument, today, price
    )
    await db.execute(
        """
        UPDATE ticker_map SET last_price = $1, last_price_date = $2, updated_at = NOW()
        WHERE instrument = $3
        """,
        price, today, instrument
    )
    await db.execute(
        """
        UPDATE holdings
        SET last_price    = $1,
            market_value  = units * $1,
            unrealized_pl = (units * $1) - total_costs,
            return_pct    = CASE WHEN total_costs > 0
                            THEN ((units * $1) - total_costs) / total_costs
                            ELSE 0 END,
            updated_at    = NOW()
        WHERE instrument = $2
        """,
        price, instrument
    )
    await db.execute(
        """
        INSERT INTO audit_log (user_id, action, table_name, record_id, new_values)
        VALUES ($1, 'PRICE_OVERRIDE', 'price_history', $2, $3::jsonb)
        """,
        admin_user_id, instrument,
        f'{{"instrument": "{instrument}", "price": {price}, "date": "{today}"}}'
    )
