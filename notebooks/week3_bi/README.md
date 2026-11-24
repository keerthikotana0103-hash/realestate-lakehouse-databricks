# Week 3 · BI engineering (Days 15–21)

| Day | Build |
| --- | --- |
| 15 | Design the star schema: `fact_listing_event`, `fact_sale` (grain = one closed sale), `fact_listing_daily_snapshot`, dims date / property / market / agent (SCD2) / office |
| 16 | Build gold tables + `agg_market_weekly` (inventory, new listings, pendings, DOM, list-to-sale, months of supply) |
| 17 | Run BI queries on a SQL warehouse; read the query profile; result cache |
| 18 | AI/BI dashboard "Market Pulse": inventory trend, DOM by metro, price-cut rate, rate vs pendings, agent leaderboard |
| 19 | Metric view with governed KPIs + a Genie space ("Which ZIPs in Austin sell fastest?") |
| 20 | Power BI: connect to the SQL warehouse, DirectQuery vs Import, rebuild 3 visuals |
| 21 | Row filter on `region` (Texas / West / Southeast managers), column mask on agent email |
