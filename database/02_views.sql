-- ============================================================
-- ZY-Invest Computed Views  v1.0.0
-- Run AFTER 01_schema.sql
-- ============================================================

CREATE OR REPLACE VIEW v_fund_overview AS
SELECT
    fs.fund_name,
    fs.inception_date,
    fs.base_currency,
    fs.fund_type,
    fs.primary_market,
    fs.financial_year_end,
    fs.total_units,
    fs.current_nta,
    fs.aum,
    fs.last_nta_date,
    (SELECT COUNT(*) FROM historical) AS trading_days,
    ROUND((fs.current_nta - 1.0) * 100, 4) AS total_return_pct,
    (SELECT COUNT(*) FROM investors WHERE is_active = TRUE) AS investor_count
FROM fund_settings fs
WHERE fs.id = 1;

CREATE OR REPLACE VIEW v_holdings_by_class AS
SELECT
    h.asset_class,
    SUM(h.market_value)    AS total_market_value,
    SUM(h.total_costs)     AS total_costs,
    SUM(h.unrealized_pl)   AS total_unrealized_pl,
    ROUND(SUM(h.market_value) / NULLIF((
        SELECT SUM(market_value) FROM holdings
    ), 0) * 100, 4) AS weight_pct
FROM holdings h
GROUP BY h.asset_class
ORDER BY total_market_value DESC;

CREATE OR REPLACE VIEW v_holdings_by_sector AS
SELECT
    COALESCE(h.sector, 'Other') AS sector,
    h.asset_class,
    SUM(h.market_value) AS total_market_value,
    ROUND(SUM(h.market_value) / NULLIF((
        SELECT SUM(market_value) FROM holdings
    ), 0) * 100, 4) AS weight_pct
FROM holdings h
GROUP BY h.sector, h.asset_class
ORDER BY total_market_value DESC;

CREATE OR REPLACE VIEW v_holdings_by_region AS
SELECT
    h.region,
    SUM(h.market_value) AS total_market_value,
    ROUND(SUM(h.market_value) / NULLIF((
        SELECT SUM(market_value) FROM holdings
    ), 0) * 100, 4) AS weight_pct
FROM holdings h
GROUP BY h.region
ORDER BY total_market_value DESC;

CREATE OR REPLACE VIEW v_investor_profile AS
SELECT
    i.id,
    i.name,
    i.units,
    i.vwap,
    i.total_costs,
    i.current_nta,
    i.market_value,
    i.unrealized_pl,
    i.realized_pl,
    i.unrealized_pl + i.realized_pl AS total_pl,
    ROUND((i.market_value - i.total_costs) /
          NULLIF(i.total_costs, 0) * 100, 4) AS simple_return_pct,
    i.irr AS irr_pct,
    i.joined_date,
    fs.current_nta AS fund_nta,
    ROUND(i.units / NULLIF(fs.total_units, 0) * 100, 4) AS fund_ownership_pct
FROM investors i
CROSS JOIN fund_settings fs
WHERE i.is_active = TRUE;

CREATE OR REPLACE VIEW v_distribution_breakdown AS
SELECT
    dl.investor_id,
    i.name AS investor_name,
    d.financial_year,
    d.title,
    d.dist_type,
    d.ex_date,
    d.pmt_date,
    d.dps_sen,
    dl.units_at_ex_date,
    dl.amount,
    dl.paid,
    dl.paid_date
FROM distribution_ledger dl
JOIN distributions d ON d.id = dl.distribution_id
JOIN investors i     ON i.id = dl.investor_id
ORDER BY d.pmt_date DESC, i.name;

CREATE OR REPLACE VIEW v_historical_nta AS
SELECT
    h.date,
    h.nta,
    h.total_units,
    h.securities + h.reits + h.bonds + h.money_market + h.derivatives AS total_securities,
    h.cash,
    h.mng_fees + h.perf_fees + h.ints_on_fees AS total_fees_accrued,
    h.loans + h.ints_on_loans AS total_liabilities,
    ROUND((h.nta / NULLIF(LAG(h.nta) OVER (ORDER BY h.date), 0) - 1) * 100, 6) AS daily_return_pct,
    ROUND((h.nta - 1.0) * 100, 4) AS cumulative_return_pct,
    h.is_locked,
    h.source
FROM historical h
ORDER BY h.date;

CREATE OR REPLACE VIEW v_fund_statement AS
WITH revenue AS (
    SELECT
        CASE
            WHEN d.pmt_date BETWEEN '2021-12-01' AND '2022-11-30' THEN 'FY22'
            WHEN d.pmt_date BETWEEN '2022-12-01' AND '2023-11-30' THEN 'FY23'
            WHEN d.pmt_date BETWEEN '2023-12-01' AND '2024-11-30' THEN 'FY24'
            WHEN d.pmt_date BETWEEN '2024-12-01' AND '2025-11-30' THEN 'FY25'
            ELSE 'FY26'
        END AS financial_year,
        SUM(d.amount) AS dividend_income
    FROM dividends d
    GROUP BY 1
),
settlement_pl AS (
    SELECT
        CASE
            WHEN s.date BETWEEN '2021-12-01' AND '2022-11-30' THEN 'FY22'
            WHEN s.date BETWEEN '2022-12-01' AND '2023-11-30' THEN 'FY23'
            WHEN s.date BETWEEN '2023-12-01' AND '2024-11-30' THEN 'FY24'
            WHEN s.date BETWEEN '2024-12-01' AND '2025-11-30' THEN 'FY25'
            ELSE 'FY26'
        END AS financial_year,
        SUM(s.profit_loss) AS realized_pl
    FROM settlement s
    GROUP BY 1
),
other_income AS (
    SELECT
        CASE
            WHEN o.record_date BETWEEN '2021-12-01' AND '2022-11-30' THEN 'FY22'
            WHEN o.record_date BETWEEN '2022-12-01' AND '2023-11-30' THEN 'FY23'
            WHEN o.record_date BETWEEN '2023-12-01' AND '2024-11-30' THEN 'FY24'
            WHEN o.record_date BETWEEN '2024-12-01' AND '2025-11-30' THEN 'FY25'
            ELSE 'FY26'
        END AS financial_year,
        SUM(CASE WHEN o.income_type = 'Interests' THEN o.amount ELSE 0 END) AS interest_income,
        SUM(CASE WHEN o.income_type != 'Interests' THEN o.amount ELSE 0 END) AS other_income
    FROM others o
    GROUP BY 1
)
SELECT
    COALESCE(r.financial_year, sp.financial_year, oi.financial_year) AS financial_year,
    COALESCE(r.dividend_income, 0)  AS dividend_income,
    COALESCE(oi.interest_income, 0) AS interest_income,
    COALESCE(sp.realized_pl, 0)     AS realized_pl,
    COALESCE(oi.other_income, 0)    AS other_income,
    COALESCE(r.dividend_income, 0) +
    COALESCE(oi.interest_income, 0) +
    COALESCE(sp.realized_pl, 0) +
    COALESCE(oi.other_income, 0)    AS total_income
FROM revenue r
FULL OUTER JOIN settlement_pl sp ON sp.financial_year = r.financial_year
FULL OUTER JOIN other_income oi  ON oi.financial_year = COALESCE(r.financial_year, sp.financial_year)
ORDER BY financial_year;

CREATE OR REPLACE VIEW v_cashflow_18m AS
SELECT
    DATE_TRUNC('month', date) AS month,
    SUM(CASE WHEN cashflow_type = 'subscription' THEN amount ELSE 0 END) AS inflow,
    SUM(CASE WHEN cashflow_type = 'redemption'   THEN amount ELSE 0 END) AS outflow,
    SUM(amount) AS net_cashflow
FROM principal_cashflows
WHERE date >= NOW() - INTERVAL '18 months'
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW v_active_fee_schedule AS
SELECT
    fee_type,
    rate,
    basis,
    hurdle_rate,
    valid_from,
    valid_to,
    description,
    ROUND(rate * 100, 4) AS rate_pct,
    CASE WHEN valid_to IS NULL OR valid_to >= CURRENT_DATE
         THEN TRUE ELSE FALSE END AS is_active
FROM fee_schedules
ORDER BY fee_type, valid_from DESC;

CREATE OR REPLACE VIEW v_price_status AS
SELECT
    tm.instrument,
    tm.yahoo_ticker,
    tm.asset_class,
    tm.is_manual,
    tm.last_price,
    tm.last_price_date,
    CASE
        WHEN tm.last_price_date IS NULL             THEN 'missing'
        WHEN tm.last_price_date < CURRENT_DATE - 1 THEN 'stale'
        ELSE 'ok'
    END AS price_status,
    CURRENT_DATE - tm.last_price_date AS days_stale
FROM ticker_map tm
JOIN holdings h ON h.instrument = tm.instrument
WHERE h.units > 0
ORDER BY price_status DESC, tm.instrument;