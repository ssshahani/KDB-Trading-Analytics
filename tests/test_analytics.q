/ ==============================================================================
/ test_analytics.q - Full Validation (all modules)
/ ==============================================================================

-1 "=== Running Full Validation ===";

/ Tick data dates (trades/quotes - last 6 months only)
td1: first date;
td2: last date;

/ Daily data dates (full 3 years)
dd1: exec min date from daily;
dd2: exec max date from daily;

-1 "Tick data range:  ", (string td1), " to ", string td2;
-1 "Daily data range: ", (string dd1), " to ", string dd2;
-1 "Tick partitions:  ", string count date;
-1 "Trades on first tick date: ", string count select from trades where date = td1;
-1 "Quotes on first tick date: ", string count select from quotes where date = td1;

/ ==============================================================================
/ DATABASE
/ ==============================================================================
-1 "";
-1 "--- Database ---";
-1 "Tables: ", " " sv string tables[];
-1 "Trade rows: ", string count trades;
-1 "Quote rows: ", string count quotes;
-1 "Daily rows: ", string count daily;
-1 "Sample trades:";
show 5 # select from trades where date = td1;

/ ==============================================================================
/ ANALYTICS MODULE (uses daily for MA/vol, ticks for VWAP/bars)
/ ==============================================================================
-1 "";
-1 "--- Analytics: VWAP (tick data) ---";
show select vwap: (sum price * size) % sum size, vol: sum size by sym from trades where date = td1;

-1 "";
-1 "--- Analytics: 5-Min Bars AAPL (tick data) ---";
show fiveMinBars[`AAPL; td1];

-1 "";
-1 "--- Analytics: Daily Bars from Ticks ---";
show dailyBarsFromTicks[td1; td1];

-1 "";
-1 "--- Analytics: Moving Averages AAPL (daily data) ---";
show 5 # select from movingAvgs[`AAPL] where not null ma20;

-1 "";
-1 "--- Analytics: Volatility AAPL (daily data) ---";
show 5 # calcVolatility[`AAPL];

-1 "";
-1 "--- Analytics: Avg Daily Volume (daily data) ---";
show avgDailyVolume[dd1; dd2];

/ ==============================================================================
/ TCA MODULE (tick data only)
/ ==============================================================================
-1 "";
-1 "--- TCA: Asof Join (trade-to-quote matching) ---";
t: select sym, time, price, size, side from trades where date = td1, sym = `AAPL;
q: select sym, time, bid, ask from quotes where date = td1, sym = `AAPL;
-1 "Trades: ", string count t;
-1 "Quotes: ", string count q;
-1 "Asof join result:";
show 5 # aj[`sym`time; t; q];

-1 "";
-1 "--- TCA: Spread Stats (tick data) ---";
show spreadStats[td1; td1];

-1 "";
-1 "--- TCA: Execution Quality (tick data) ---";
show execQuality[td1; td1];

/ ==============================================================================
/ RISK MODULE (daily data for VaR/vol, tick data for exposure)
/ ==============================================================================
/ -1 "";
/ -1 "--- Risk: Historical VaR AAPL 95% 1-day (daily data) ---";
/ show historicalVaR[`AAPL; 0.95; 1; dd1; dd2];

/ -1 "";
/ -1 "--- Risk: Parametric VaR AAPL 95% 1-day (daily data) ---";
/ show parametricVaR[`AAPL; 0.95; 1; dd1; dd2];

/ -1 "";
/ -1 "--- Risk: Sector Exposure (tick data) ---";
/ show sectorExposure[td1];

/ -1 "";
/ -1 "--- Risk: PnL by Sector (daily data) ---";
/ show pnlBySector[dd1; dd2];

/ ==============================================================================
/ ALERTS MODULE (daily data)
/ ==============================================================================
-1 "";
-1 "--- Alerts: Volume Anomalies ---";
va: volumeAlerts[20; 2.0];
-1 "Volume alerts found: ", string count va;
show 5 # va;

-1 "";
-1 "--- Alerts: Price Gaps ---";
pg: priceGapAlerts[2.0];
-1 "Price gap alerts found: ", string count pg;
show 5 # pg;

-1 "";
-1 "--- Alerts: Volatility Breakouts ---";
vb: volatilityAlerts[20; 2.0];
-1 "Volatility alerts found: ", string count vb;
show 5 # vb;

/ ==============================================================================
/ REPORTS MODULE (mix of daily and tick data)
/ ==============================================================================
/ -1 "";
/ -1 "--- Reports: Top 5 Performers (daily data) ---";
/ topPerformers[5; dd1; dd2];

-1 "";
-1 "--- Reports: Daily Summary (tick data) ---";
dailySummary[td1];

-1 "";
-1 "--- Reports: Top 10 Most Active (tick data) ---";
show topActive[10; td1];

-1 "";
-1 "========================================";
-1 "  ALL MODULE TESTS COMPLETE";
-1 "========================================";
