/ ==============================================================================
/ utils.q - Utility Functions & Common Helpers
/ ==============================================================================
/ Concepts: functions, lambdas, error handling, composition, projections
/ ==============================================================================

/ ==============================================================================
/ PRINTING
/ ==============================================================================
/ In Q, there are two ways to print:
/   show x   - prints any Q object (tables, lists, atoms)
/   -1 "x"   - prints a STRING to stdout as a clean line
/
/ IMPORTANT: show "hello" prints each character on its own line!
/   Because a string is a list of chars, and show prints list items separately.
/   Always use -1 for string messages.

/ ==============================================================================
/ LOGGING
/ ==============================================================================
logInfo:  {-1 "[INFO]  [", (string .z.T), "] ", x;}
logWarn:  {-1 "[WARN]  [", (string .z.T), "] ", x;}
logError: {-1 "[ERROR] [", (string .z.T), "] ", x;}

/ ==============================================================================
/ TIMING - Measure query performance
/ ==============================================================================
timeIt: {[name; f]
    t0: .z.T;
    res: @[f; ::; {[e] logError "Failed: ", e; :()}];
    elapsed: .z.T - t0;
    logInfo name, " completed in ", string elapsed;
    res
    }

/ ==============================================================================
/ DATE HELPERS
/ ==============================================================================
tradingDays: {[startDate; endDate]
    allDates: startDate + til 1 + endDate - startDate;
    allDates where not (allDates mod 7) in 0 6
    }

prevTradingDay: {[d]
    d: d - 1;
    while[(d mod 7) in 0 6; d: d - 1];
    d
    }

bizDayCount: {[s; e] count tradingDays[s; e]}

/ ==============================================================================
/ FINANCIAL MATH HELPERS
/ ==============================================================================

/ Returns from prices
calcReturns: {1 _ deltas[x] % prev x}

/ Annualized volatility from daily returns
annualVol: {(dev x) * sqrt 252.0}

/ Sharpe ratio (assuming 0 risk-free rate)
sharpeRatio: {(avg x) % (dev x) * sqrt 252.0}

/ Drawdown from cumulative returns
maxDrawdown: {
    cumRet: prds 1.0 + x;
    runMax: maxs cumRet;
    min (cumRet - runMax) % runMax
    }

/ Mid price from bid/ask
midPrice: {(x + y) % 2.0}

/ Spread in basis points
spreadBps: {10000.0 * (y - x) % midPrice[x; y]}

/ ==============================================================================
/ TABLE HELPERS
/ ==============================================================================
tableExists: {[t] (t in tables[]) and 0 < count value t}
getSyms: {exec distinct sym from x}
safeDivide: {$[y=0; 0.0; x % y]}

/ ==============================================================================
/ DISPLAY HELPERS
/ ==============================================================================
/ Use -1 for strings, show for tables/data

reportHeader: {[title]
    -1 "";
    -1 60#"=";
    -1 "  ", title;
    -1 60#"=";
    -1 "";
    }

-1 "utils.q loaded - utility functions ready";
