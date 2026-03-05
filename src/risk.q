/ ==============================================================================
/ risk.q - Risk Analytics
/ ==============================================================================
/ Concepts: left joins for enrichment, aggregation, portfolio analytics,
/           VaR (Value at Risk), sector exposure, PnL attribution
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. VALUE AT RISK (VaR)
/ ==============================================================================
/ VaR answers: "What is the maximum loss at X% confidence over N days?"

/ Historical VaR: use actual historical returns
historicalVaR: {[s; confidence; nDays; startDate; endDate]
    / Get daily returns
    rets: exec ret from
        (update ret: 1 _ deltas[close] % prev close
         from select date, close from daily where sym = s, date within (startDate; endDate))
    where not null ret;

    / Sort returns ascending (worst first)
    sortedRets: asc rets;

    / VaR = the (1-confidence) percentile return * sqrt(nDays) for scaling
    idx: `long$ (1 - confidence) * count sortedRets;
    dailyVaR: sortedRets[idx];
    nDayVaR: dailyVaR * sqrt `float$nDays;

    `confidence`holdingDays`dailyVaR`nDayVaR`numObs !
        (confidence; nDays; dailyVaR; nDayVaR; count rets)
    }

/ Parametric VaR: assumes normal distribution
parametricVaR: {[s; confidence; nDays; startDate; endDate]
    rets: exec ret from
        (update ret: 1 _ deltas[close] % prev close
         from select date, close from daily where sym = s, date within (startDate; endDate))
    where not null ret;

    mu: avg rets;
    sigma: dev rets;

    / Z-scores for common confidence levels
    zScores: 0.90 0.95 0.99 ! 1.282 1.645 2.326;
    z: zScores confidence;

    dailyVaR: mu - z * sigma;
    nDayVaR: (mu * nDays) - z * sigma * sqrt `float$nDays;

    `confidence`holdingDays`mean`stdev`zScore`dailyVaR`nDayVaR !
        (confidence; nDays; mu; sigma; z; dailyVaR; nDayVaR)
    }

/ Portfolio VaR (assuming equal weight portfolio)
portfolioVaR: {[syms; confidence; startDate; endDate]
    / Get returns matrix
    retsBySyms: {[s; sd; ed]
        exec ret from
            (update ret: 1 _ deltas[close] % prev close
             from select date, close from daily where sym = s, date within (sd; ed))
        where not null ret
    }[; startDate; endDate] each syms;

    / Equal weighted portfolio return
    n: min count each retsBySyms;
    portRet: avg each flip (n#) each retsBySyms;

    sortedPortRet: asc portRet;
    idx: `long$ (1 - confidence) * count sortedPortRet;

    `confidence`portfolioVaR`avgReturn`volatility`numObs !
        (confidence; sortedPortRet idx; avg portRet; dev portRet; n)
    }

/ ==============================================================================
/ 2. SECTOR EXPOSURE
/ ==============================================================================
/ Uses left join (lj) to enrich trades with sector from reference data

sectorExposure: {[d]
    / Get last price and total volume per symbol for the day
    positions: select lastPx: last price, totalVol: sum size by sym
               from trades where date = d;

    / LEFT JOIN with reference data to get sector
    / lj adds columns from keyed table where keys match
    enriched: positions lj refData;

    / Calculate notional exposure per sector
    enriched: update notional: lastPx * totalVol from enriched;

    sectorExp: select totalNotional: sum notional,
                      numStocks: count i,
                      avgNotional: avg notional
               by sector from enriched;

    / Add portfolio weight
    totalNot: exec sum totalNotional from sectorExp;
    update weight: totalNotional % totalNot from sectorExp
    }

/ ==============================================================================
/ 3. PROFIT & LOSS (PnL)
/ ==============================================================================

/ Daily PnL from close-to-close returns
dailyPnL: {[syms; startDate; endDate]
    t: select date, sym, close from daily where sym in syms, date within (startDate; endDate);
    t: update ret: 1 _ deltas[close] % prev close by sym from t;
    select pnlPct: ret, cumPnl: prds 1.0 + ret by sym from t where not null ret
    }

/ PnL attribution by sector
pnlBySector: {[startDate; endDate]
    t: update ret: 1 _ deltas[close] % prev close by sym from
       select date, sym, close from daily where date within (startDate; endDate);
    / Enrich with sector via left join
    t: t lj `sym xkey select sym, sector from refData;
    select avgReturn: avg ret, totalReturn: sum ret, stocks: count distinct sym
    by sector from t where not null ret
    }

/ ==============================================================================
/ 4. DRAWDOWN ANALYSIS
/ ==============================================================================

drawdownAnalysis: {[s; startDate; endDate]
    t: select date, close from daily where sym = s, date within (startDate; endDate);
    t: update ret: 1 _ deltas[close] % prev close from t;
    t: update cumRet: prds 1.0 + ret from t where not null ret;
    t: update runMax: maxs cumRet from t;
    t: update drawdown: (cumRet - runMax) % runMax from t;

    show "=== Drawdown Analysis: ", string s, " ===";
    show "Max drawdown: ", string exec min drawdown from t;
    show "Current drawdown: ", string exec last drawdown from t;
    t
    }

/ ==============================================================================
/ 5. RISK DASHBOARD
/ ==============================================================================

riskDashboard: {[syms; startDate; endDate]
    reportHeader "Risk Dashboard";

    / VaR for each symbol
    show "=== Value at Risk (95%, 1-day) ===";
    varResults: {[s; sd; ed]
        r: historicalVaR[s; 0.95; 1; sd; ed];
        `sym`dailyVaR ! (s; r`dailyVaR)
    }[; startDate; endDate] each syms;
    show ([] sym: syms) ,' ([] dailyVaR95: varResults@\:`dailyVaR);

    / Volatility
    show "";
    show "=== Annualized Volatility ===";
    {[s; sd; ed]
        rets: exec ret from (update ret: 1 _ deltas[close] % prev close from select date, close from daily where sym = s, date within (sd; ed)) where not null ret;
        show "  ", (string s), ": ", string (dev rets) * sqrt 252.0;
    }[; startDate; endDate] each syms;

    / Sector exposure (latest date)
    show "";
    show "=== Sector Exposure (latest date) ===";
    show sectorExposure[endDate];

    show "";
    show "Risk dashboard complete";
    }

show "risk.q loaded - risk analytics ready";
show "  historicalVaR[`AAPL; 0.95; 1; sd; ed]  - Historical VaR";
show "  parametricVaR[`AAPL; 0.95; 1; sd; ed]  - Parametric VaR";
show "  sectorExposure[date]                     - Sector notional exposure";
show "  riskDashboard[syms; sd; ed]              - Full risk dashboard";
