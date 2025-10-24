-- Example KPI queries (adapt in BI tool / scheduled jobs)
-- Null/format violation rate per day would be stored from reports; here we show basic business KPIs.
SELECT date_trunc('day', order_date) AS day, SUM(amount) AS revenue, COUNT(*) AS orders
FROM dq_demo.transactions
GROUP BY 1
ORDER BY 1;
