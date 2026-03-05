/ ==============================================================================
/ analytics.q - Core Trading Analytics
/ ==============================================================================
/ Concepts: qSQL aggregations, xbar bucketing, moving averages (mavg/mdev),
/           VWAP/TWAP, rolling volatility, returns, functional select
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. VWAP - Volume Weighted Average Price
/ ==============================================================================
/ VWAP = sum(price * volume) / sum(volume)
/ Used by traders to assess execution quality vs market average

/ Daily VWAP per symbol
calcVWAP: {[s; startDate; endDate]
    select vwap: (sum price * size) % sum size,
           volume: sum size,
           trades: count i
    by date
    from trades
    where sym = s, date within (startDate; endDate)
    }

/ Intraday VWAP (cumulative throughout the day)
intradayVWAP: {[s; d]
    t: select time, price, size from trades where sym = s, date = d;
    t: update cumVwap: (sums price * size) % sums size from t;
    t
    }

/ VWAP for all symbols in a date range
allVWAP: {[startDate; endDate]
    select vwap: (sum price * size) % sum size,
           volume: sum size,
           trades: count i
    by sym, date
    from trades
    where date within (startDate; endDate)
    }

/ ==============================================================================
/ 2. TWAP - Time Weighted Average Price
/ ==============================================================================
/ TWAP = simple average of prices over time (ignores volume)
/ Used as a benchmark for algorithmic execution

calcTWAP: {[s; startDate; endDate]
    select twap: avg price, trades: count i
    by date
    from trades
    where sym = s, date within (startDate; endDate)
    }

/ ==============================================================================
/ 3. OHLC BARS - Open/High/Low/Close
/ ==============================================================================
/ xbar is the key function for time bucketing
/ n xbar x rounds x down to nearest multiple of n

/ Build n-minute OHLC bars
buildBars: {[s; d; nMinutes]
    select open: first price, high: max price, low: min price,
           close: last price, volume: sum size, trades: count i
    by bar: nMinutes xbar time.minute
    from trades
    where sym = s, date = d
    }

/ 5-minute bars (most common)
fiveMinBars: {[s; d] buildBars[s; d; 5]}

/ 1-minute bars (for detailed analysis)
oneMinBars: {[s; d] buildBars[s; d; 1]}

/ Daily bars from tick data
dailyBarsFromTicks: {[startDate; endDate]
    select open: first price, high: max price, low: min price,
           close: last price, volume: sum size, trades: count i
    by sym, date
    from trades
    where date within (startDate; endDate)
    }

/ ==============================================================================
/ 4. MOVING AVERAGES
/ ==============================================================================
/ mavg[n; list] = n-period simple moving average
/ ema[n; list] = exponential moving average (via scan)

/ Add moving averages to daily close prices
movingAvgs: {[s]
    t: select date, close from daily where sym = s;
    update ma5:   mavg[5; close],
           ma10:  mavg[10; close],
           ma20:  mavg[20; close],
           ma50:  mavg[50; close],
           ma200: mavg[200; close]
    from t
    }


/ Detect golden cross (MA50 crosses above MA200) and death cross (opposite)
crossoverSignals: {[s]
    t: movingAvgs[s];
    t: update signal: ?[ma50 > ma200; `golden; ?[ma50 < ma200; `death; `neutral]] from t;
    / Find where signal changes
    t: update crossover: signal <> prev signal from t;
    select from t where crossover
    }

/ ==============================================================================
/ 5. VOLATILITY
/ ==============================================================================
/ Rolling volatility = standard deviation of returns * sqrt(252)

/ Daily returns and volatility
calcVolatility: {[s]
    t: select date, close from daily where sym = s;
    t: update ret: deltas[close] % prev close from t;
    t: update vol20: (mdev[20; ret]) * sqrt 252.0 from t;
    select from t where not null vol20
    }

/ Realized volatility for all symbols (latest)
allVolatility: {[]
    / Calculate returns per symbol from daily data
    t: update ret: 1 _ deltas[close] % prev close by sym from daily;
    select vol20: (dev ret) * sqrt 252.0,
           vol60: (last 60 # ret) {(dev x) * sqrt 252.0} ' ret,
           lastClose: last close
    by sym
    from t
    where not null ret
    }

/ Intraday volatility (from tick data)
intradayVol: {[s; d]
    t: select time, price from trades where sym = s, date = d;
    t: update ret: 1 _ deltas[price] % prev price from t;
    select intradayVol: dev ret,
           annualized: (dev ret) * sqrt 252.0 * sqrt count ret
    from t
    where not null ret
    }

/ ==============================================================================
/ 6. VOLUME ANALYTICS
/ ==============================================================================

/ Average daily volume per symbol
avgDailyVolume: {[startDate; endDate]
    select adv: avg volume, totalVol: sum volume, days: count i
    by sym
    from daily
    where date within (startDate; endDate)
    }

/ Volume profile by time of day (when does volume concentrate?)
volumeProfile: {[s; startDate; endDate]
    select vol: sum size, trades: count i, avgSize: avg size
    by bucket: 30 xbar time.minute
    from trades
    where sym = s, date within (startDate; endDate)
    }

/ Relative volume (today vs 20-day average)
relativeVolume: {[d]
    todayVol: select todayVol: sum size by sym from trades where date = d;
    hist: select adv20: avg volume by sym from daily where date within (d-30; d-1);
    result: todayVol lj hist;
    update rvol: todayVol % adv20 from result
    }

/ ==============================================================================
/ 7. CORRELATION & BETA
/ ==============================================================================

/ Pairwise correlation matrix from daily returns
corrMatrix: {[syms; startDate; endDate]
    / Get returns for each symbol
    rets: {[s; sd; ed]
        exec ret from
            (update ret: 1 _ deltas[close] % prev close from select date, close from daily where sym = s, date within (sd; ed))
        where not null ret
    }[; startDate; endDate] each syms;

    / Build correlation matrix
    n: count syms;
    matrix: {[rets; i; j] cor[rets i; rets j]}[rets;;] each/: til n;
    ([] sym: syms) , flip syms ! matrix
    }

/ Beta vs a benchmark symbol (e.g., SPY proxy using first symbol)
calcBeta: {[s; benchSym; startDate; endDate]
    sRet: exec ret from (update ret: 1 _ deltas[close] % prev close from select date, close from daily where sym = s, date within (startDate; endDate)) where not null ret;
    bRet: exec ret from (update ret: 1 _ deltas[close] % prev close from select date, close from daily where sym = benchSym, date within (startDate; endDate)) where not null ret;
    n: min (count sRet; count bRet);
    cov[n # sRet; n # bRet] % var n # bRet
    }

/ ==============================================================================
/ 8. FUNCTIONAL SELECT (dynamic queries)
/ ==============================================================================
/ When column names are variables, use functional form: ?[table; where; by; select]

/ Dynamic aggregation - pass column name as parameter
dynamicAgg: {[tableName; aggFunc; colName; groupCol]
    / Functional select: ?[table; whereClause; byClause; selectClause]
    ?[tableName; (); (enlist groupCol) ! enlist groupCol; (enlist `result) ! enlist (aggFunc; colName)]
    }
/ Usage: dynamicAgg[`trades; avg; `price; `sym]

show "analytics.q loaded - core analytics ready";
show "  calcVWAP[`AAPL; 2024.01.01; 2024.06.30]   - VWAP by date";
show "  fiveMinBars[`AAPL; 2024.06.14]             - 5-min OHLC bars";
show "  movingAvgs[`AAPL]                           - MA5/10/20/50/200";
show "  calcVolatility[`AAPL]                       - Rolling volatility";
show "  corrMatrix[`AAPL`MSFT`GOOG; sd; ed]         - Correlation matrix";
