"""
Price fetcher service  v2.0.0
Fetches daily closing prices via yfinance.
Falls back gracefully — marks instruments as needing manual input.
"""
import asyncio
from datetime import date
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


def _yfinance_fetch_sync(tickers: List[str]) -> Dict[str, float]:
    """Synchronous yfinance fetch — run in thread executor."""
    try:
        import yfinance as yf
        prices = {}
        for ticker in tickers:
            try:
                t = yf.Ticker(ticker)
                info = t.fast_info
                price = getattr(info, 'last_price', None)
                if price and float(price) > 0:
                    prices[ticker] = float(price)
                    logger.info(f"Fetched {ticker}: {price}")
                else:
                    # Fallback: try history
                    hist = t.history(period="2d")
                    if not hist.empty:
                        prices[ticker] = float(hist['Close'].iloc[-1])
                        logger.info(f"Fetched {ticker} via history: {prices[ticker]}")
                    else:
                        logger.warning(f"No price data for {ticker}")
            except Exception as e:
                logger.warning(f"Failed to fetch {ticker}: {e}")
        return prices
    except ImportError:
        logger.error("yfinance not installed — run: pip install yfinance")
        return {}
    except Exception as e:
        logger.error(f"yfinance fetch error: {e}")
        return {}


async def fetch_prices_yahoo(tickers: List[str]) -> Dict[str, float]:
    """Fetch latest prices for a list of Yahoo Finance tickers."""
    if not tickers:
        return {}
    try:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _yfinance_fetch_sync, tickers)
    except Exception as e:
        logger.error(f"Price fetch failed: {e}")
        return {}


async def run_daily_price_fetch(db) -> dict:
    """
    Main daily price fetch routine.
    Called by scheduler at 6:05 PM MYT after Bursa closes.
    """
    today = date.today()

    # Get all active instruments with Yahoo tickers
    instruments = await db.fetch("""
        SELECT tm.instrument, tm.yahoo_ticker, tm.is_manual
        FROM ticker_map tm
        JOIN holdings h ON h.instrument = tm.instrument
        WHERE h.units > 0
    """)

    if not instruments:
        logger.warning("No active holdings found in ticker_map")
        return {"date": str(today), "succeeded": [], "failed": [],
                "manual_needed": [], "total_fetched": 0, "needs_attention": 0,
                "error": "No active holdings in ticker_map"}

    auto_tickers    = []
    manual_needed   = []
    ticker_to_instr = {}

    for row in instruments:
        if not row["is_manual"] and row["yahoo_ticker"]:
            auto_tickers.append(row["yahoo_ticker"])
            ticker_to_instr[row["yahoo_ticker"]] = row["instrument"]
        else:
            manual_needed.append(row["instrument"])

    logger.info(f"Fetching prices for: {auto_tickers}")

    fetched   = await fetch_prices_yahoo(auto_tickers)
    succeeded = []
    failed    = []

    for yahoo_ticker, price in fetched.items():
        instrument = ticker_to_instr.get(yahoo_ticker)
        if not instrument:
            continue
        try:
            await db.execute("""
                INSERT INTO price_history (instrument, date, price, source)
                VALUES ($1, $2, $3, 'yahoo')
                ON CONFLICT (instrument, date) DO UPDATE SET
                    price = EXCLUDED.price, source = 'yahoo'
            """, instrument, today, price)

            await db.execute("""
                UPDATE ticker_map
                SET last_price = $1, last_price_date = $2, updated_at = NOW()
                WHERE instrument = $3
            """, price, today, instrument)

            await db.execute("""
                UPDATE holdings
                SET last_price    = $1,
                    market_value  = units * $1,
                    unrealized_pl = (units * $1) - total_costs,
                    return_pct    = CASE WHEN total_costs > 0
                                    THEN ((units * $1) - total_costs) / total_costs
                                    ELSE 0 END,
                    updated_at    = NOW()
                WHERE instrument  = $2
            """, price, instrument)

            succeeded.append(f"{instrument} @ RM {price:.4f}")
        except Exception as e:
            logger.error(f"Failed to save price for {instrument}: {e}")
            failed.append(instrument)

    # Mark unfetched tickers as failed
    for yahoo_ticker in auto_tickers:
        if yahoo_ticker not in fetched:
            instrument = ticker_to_instr.get(yahoo_ticker)
            if instrument:
                failed.append(f"{instrument} ({yahoo_ticker} — not returned by Yahoo)")

    logger.info(f"Price fetch complete: {len(succeeded)} succeeded, {len(failed)} failed")

    return {
        "date":            str(today),
        "succeeded":       succeeded,
        "failed":          failed,
        "manual_needed":   manual_needed,
        "total_fetched":   len(succeeded),
        "needs_attention": len(failed) + len(manual_needed),
        "tickers_tried":   auto_tickers,
    }


async def update_manual_price(db, instrument: str, price: float, admin_user_id: str):
    """Admin manually sets price for OTC/warrant instruments."""
    today = date.today()
    await db.execute("""
        INSERT INTO price_history (instrument, date, price, source)
        VALUES ($1, $2, $3, 'admin_manual')
        ON CONFLICT (instrument, date) DO UPDATE
        SET price = EXCLUDED.price, source = 'admin_manual'
    """, instrument, today, price)

    await db.execute("""
        UPDATE ticker_map
        SET last_price = $1, last_price_date = $2, updated_at = NOW()
        WHERE instrument = $3
    """, price, today, instrument)

    await db.execute("""
        UPDATE holdings
        SET last_price    = $1,
            market_value  = units * $1,
            unrealized_pl = (units * $1) - total_costs,
            return_pct    = CASE WHEN total_costs > 0
                            THEN ((units * $1) - total_costs) / total_costs
                            ELSE 0 END,
            updated_at    = NOW()
        WHERE instrument = $2
    """, price, instrument)

    await db.execute("""
        INSERT INTO audit_log (user_id, action, table_name, record_id, new_values)
        VALUES ($1, 'PRICE_OVERRIDE', 'price_history', $2, $3::jsonb)
    """, admin_user_id, instrument,
        f'{{"instrument": "{instrument}", "price": {price}, "date": "{today}"}}')
