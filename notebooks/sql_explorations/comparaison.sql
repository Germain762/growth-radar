-- Requête 1 : Comparaison court terme vs long terme
-- Cherche les secteurs qui performent à court terme MAIS pas à long terme
-- (= rotation sectorielle en cours)
SELECT
    gics_sector,
    ROUND(avg_return_5d * 100, 2)   AS ret_1w,S
    ROUND(avg_return_21d * 100, 2)  AS ret_1m,
    ROUND(avg_return_63d * 100, 2)  AS ret_3m,
    ROUND(avg_return_252d * 100, 2) AS ret_1y,
    -- Signal de retournement : court terme positif, long terme négatif
    CASE
        WHEN avg_return_21d > 0 AND avg_return_252d < 0 THEN 'Recovery 📈'
        WHEN avg_return_21d < 0 AND avg_return_252d > 0 THEN 'Pullback 📉'
        WHEN avg_return_21d > 0 AND avg_return_252d > 0 THEN 'Trending up'
        ELSE 'Weak'
    END AS signal
FROM mart_momentum_gics
ORDER BY avg_return_21d DESC;

-- Requête 2 : Volatilité vs performance (Sharpe-like)
-- Quel secteur offre le meilleur ratio rendement/risque ?
SELECT
    gics_sector,
    ROUND(avg_return_21d * 100, 2)    AS ret_pct,
    ROUND(avg_volatility_21d * 100, 2) AS vol_pct,
    ROUND(avg_return_21d / NULLIF(avg_volatility_21d, 0), 2) AS return_to_risk_ratio
FROM mart_momentum_gics
WHERE avg_volatility_21d > 0
ORDER BY return_to_risk_ratio DESC;

-- Requête 3 : Drilldown sur un secteur précis
-- Qui sont les meilleurs/pires constituants ?
SELECT
    d.gics_sector,
    d.ticker_nk,
    d.long_name,
    ROUND(f.return_21d * 100, 2) AS ret_1m_pct,
    ROUND(f.close_price, 2)      AS price
FROM fct_price_daily f
JOIN dim_instrument d ON f.instrument_sk = d.instrument_sk
WHERE f.price_date = (SELECT MAX(price_date) FROM fct_price_daily)
  AND d.gics_sector = 'Technology'  -- adapte selon tes données
  AND d.instrument_type = 'equity'
ORDER BY f.return_21d DESC;
