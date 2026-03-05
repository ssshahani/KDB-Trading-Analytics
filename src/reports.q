/ ==============================================================================
/ reports.q - Business Reports & PM Dashboard
/ ==============================================================================
/ Translating business questions into KDB+ queries.
/ Concepts: complex aggregations, multiple joins, report generation
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. PM DASHBOARD - Portfolio Manager's Daily View
/ ==============================================================================

pmDashboard: {[syms; startDate; endDate]
    reportHeader "Portfolio Manager Dashboard";
    show "Period: ", (string startDate), " to ", string endDate;
    show "Universe: ", " " sv string syms;
    show "";

    / --- Price Performance ---
    show "=== Price Performance ===";
    perf: select firstClose: first close, lastClose: last close,
                 totalReturn: 100.0 * (last[close] - first close) % first close,
                 high52w: max high, low52w: min low
          by sym
          from daily
          where sym in syms, date within (startDate; endDate);
    show `totalReturn xdesc perf;

    / --- Enriched with sector (left join) ---
    show "";
    show "=== Performance by Sector ===";
    perfSector: perf lj select sector from refData;
    show select avgReturn: avg totalReturn, bestStock: sym where totalReturn = max totalReturn
         by sector from perfSector;

    / --- Volume Analysis ---
    show "";
    show "=== Volume Statistics ===";
    show select avgDailyVol: avg volume, totalVol: sum volume, tradingDays: count i
         by sym
         from daily
         where sym in syms, date within (startDate; endDate);

    / --- Volatility Ranking ---
    show "";
    show "=== Volatility Ranking (annualized) ===";
    volRank: select vol: (dev 1 _ deltas[close] % prev close) * sqrt 252.0
             by sym from daily
             where sym in syms, date within (startDate; endDate);
    show `vol xdesc volRank;

    show "";
    show "Dashboard complete";
    }

/ ==============================================================================
/ 2. BEST/WORST PERFORMERS
/ ==============================================================================

topPerformers: {[n; startDate; endDate]
    perf: select totalReturn: 100.0 * (last[close] - first close) % first close
          by sym from daily where date within (startDate; endDate);
    perf: perf lj select sector, name from refData;
    show "=== Top ", (string n), " Performers ===";
    show n # `totalReturn xdesc perf;
    show "";
    show "=== Bottom ", (string n), " Performers ===";
    show n # `totalReturn xasc perf;
    }

/ ==============================================================================
/ 3. SECTOR ROTATION REPORT
/ ==============================================================================
/ Monthly performance by sector - shows where money is flowing

sectorRotation: {[startDate; endDate]
    reportHeader "Sector Rotation";

    / Monthly returns by symbol
    monthly: select monthRet: 100.0 * (last[close] - first close) % first close
             by sym, month: `month$date
             from daily
             where date within (startDate; endDate);

    / Join with sector
    monthly: monthly lj `sym xkey select sym, sector from refData;

    / Aggregate by sector and month
    sectorMonthly: select avgRet: avg monthRet, stocks: count distinct sym
                   by sector, month from monthly;

    show sectorMonthly;
    }

/ ==============================================================================
/ 4. LIQUIDITY REPORT
/ ==============================================================================

liquidityReport: {[startDate; endDate]
    reportHeader "Liquidity Report";

    show "=== Average Daily Volume ===";
    adv: select adv: avg volume, avgTrades: avg count_i
         by sym from
         (select volume, count_i: count i by sym, date from trades where date within (startDate; endDate));
    show `adv xdesc adv;

    show "";
    show "=== Average Spread (bps) ===";
    spreads: select avgSpreadBps: avg 10000.0 * (ask - bid) % (bid + ask) % 2
             by sym from quotes
             where date within (startDate; endDate), bid > 0, ask > bid;
    show `avgSpreadBps xasc spreads;
    }

/ ==============================================================================
/ 5. DAILY TRADING SUMMARY
/ ==============================================================================

dailySummary: {[d]
    reportHeader "Daily Trading Summary: ", string d;

    / Trade statistics
    show "=== Trade Statistics ===";
    show select trades: count i, volume: sum size,
                avgSize: avg size, avgPrice: avg price,
                vwap: (sum price * size) % sum size
         by sym from trades where date = d;

    / Market movers (biggest price change)
    show "";
    show "=== Market Movers ===";
    movers: select openPx: first price, closePx: last price,
                   changePct: 100.0 * (last[price] - first price) % first price
            by sym from trades where date = d;
    show 5 # `changePct xdesc movers;
    show "---";
    show 5 # `changePct xasc movers;

    / Volume leaders
    show "";
    show "=== Volume Leaders ===";
    show 5 # `volume xdesc select volume: sum size by sym from trades where date = d;

    / Spread summary
    show "";
    show "=== Spread Summary (bps) ===";
    show select avgSpreadBps: avg 10000.0 * (ask - bid) % (bid + ask) % 2
         by sym from quotes where date = d, bid > 0, ask > bid;
    }

/ ==============================================================================
/ 6. CORRELATION HEATMAP DATA
/ ==============================================================================

corrReport: {[syms; startDate; endDate]
    reportHeader "Correlation Report";
    show "Symbols: ", " " sv string syms;
    show "Period:  ", (string startDate), " to ", string endDate;
    show "";

    / Build correlation matrix
    rets: {[s; sd; ed]
        exec ret from
            (update ret: 1 _ deltas[close] % prev close
             from select date, close from daily where sym = s, date within (sd; ed))
        where not null ret
    }[; startDate; endDate] each syms;

    n: count syms;
    minLen: min count each rets;
    trimmedRets: (minLen #) each rets;

    / Build matrix
    matrix: {[r; i; j] cor[r i; r j]}[trimmedRets;;] each/: til n;
    result: ([] sym: syms) , flip syms ! matrix;

    show "=== Correlation Matrix ===";
    show result;
    result
    }

/ ==============================================================================
/ 7. CUSTOM QUERY BUILDER (for ad-hoc BA requests)
/ ==============================================================================
/ Demonstrates functional select for dynamic queries

/ "Show me the top N most active stocks on date D"
topActive: {[n; d]
    n # `totalVol xdesc
        select totalVol: sum size, numTrades: count i, vwap: (sum price*size) % sum size
        by sym from trades where date = d
    }

/ "What was the average spread for tech stocks last month?"
sectorSpread: {[sectorName; startDate; endDate]
    techSyms: exec sym from refData where sector = sectorName;
    select avgSpreadBps: avg 10000.0 * (ask - bid) % (bid + ask) % 2
    by sym from quotes
    where sym in techSyms, date within (startDate; endDate), bid > 0, ask > bid
    }

/ "Compare VWAP to closing price - were we buying high?"
vwapVsClose: {[startDate; endDate]
    vwaps: select vwap: (sum price * size) % sum size by sym, date from trades where date within (startDate; endDate);
    closes: select closePx: last price by sym, date from trades where date within (startDate; endDate);
    result: vwaps lj closes;
    update vwapVsClose: 10000.0 * (vwap - closePx) % closePx from result
    }

show "reports.q loaded - business reports ready";
show "  pmDashboard[syms; sd; ed]        - Full PM dashboard";
show "  topPerformers[5; sd; ed]         - Best/worst N stocks";
show "  sectorRotation[sd; ed]           - Monthly sector performance";
show "  dailySummary[date]               - Daily trading summary";
show "  corrReport[syms; sd; ed]         - Correlation matrix";
show "  topActive[10; date]              - Top N most active stocks";
