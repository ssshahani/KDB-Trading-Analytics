/ ==============================================================================
/ tca.q - Transaction Cost Analysis
/ ==============================================================================
/ THE most important analytics for front office trading.
/ Concepts: asof join (aj), window join (wj), spread analysis,
/           slippage, market impact, implementation shortfall
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. ASOF JOIN - Match Trades to Prevailing Quotes
/ ==============================================================================
/ For each trade, find the quote that was live at that exact moment
/ aj[`sym`time; trades; quotes] - matches on sym AND most recent time

/ Core TCA: enrich trades with quote data at time of execution
enrichTrades: {[startDate; endDate]
    / Get trades and quotes for the period
    t: select sym, date, time, price, size, side from trades where date within (startDate; endDate);
    q: select sym, date, time, bid, ask, bidSize, askSize from quotes where date within (startDate; endDate);

    / THE ASOF JOIN - one line, matches each trade to latest quote
    enriched: aj[`sym`time; t; q];

    / Calculate execution quality metrics
    enriched: update
        mid: (bid + ask) % 2.0,                              / midpoint at time of trade
        spread: ask - bid,                                    / spread at time of trade
        spreadBps: 10000.0 * (ask - bid) % (bid + ask) % 2,  / spread in basis points
        / Slippage: how much worse than mid did we execute?
        slippage: ?[side = `buy;
            price - (bid + ask) % 2.0;                        / buy: paid above mid = bad
            (bid + ask) % 2.0 - price],                       / sell: received below mid = bad
        / Did we cross the spread? (execute at or beyond the far side)
        crossedSpread: ?[side = `buy;
            price >= ask;                                      / bought at ask or higher
            price <= bid]                                      / sold at bid or lower
    from enriched;

    / Slippage in bps
    enriched: update slippageBps: 10000.0 * slippage % mid from enriched;

    enriched
    }

/ ==============================================================================
/ 2. WINDOW JOIN - Average Market Around Each Trade
/ ==============================================================================
/ wj aggregates quotes within a time window around each trade
/ Example: average bid/ask in the 5 seconds before each trade

windowAnalysis: {[s; d; windowSecs]
    t: select sym, time, price, size, side from trades where sym = s, date = d;
    q: select sym, time, bid, ask from quotes where sym = s, date = d;

    / Build time windows: (tradeTime - windowSecs; tradeTime) for each trade
    windows: (t`time) -\: `time$1000 * windowSecs 0;

    / Window join: for each trade, avg bid/ask in the window before it
    wj[windows; `sym`time # t; (q; (avg; `bid); (avg; `ask))]
    }

/ ==============================================================================
/ 3. SPREAD ANALYSIS
/ ==============================================================================

/ Average spread by symbol and time of day
spreadByTime: {[startDate; endDate]
    select avgSpread: avg ask - bid,
           avgSpreadBps: avg 10000.0 * (ask - bid) % (bid + ask) % 2,
           quoteCount: count i
    by sym, bucket: 30 xbar time.minute
    from quotes
    where date within (startDate; endDate)
    }

/ Spread statistics per symbol
spreadStats: {[startDate; endDate]
    select avgSpreadBps: avg 10000.0 * (ask - bid) % (bid + ask) % 2,
           medSpreadBps: med 10000.0 * (ask - bid) % (bid + ask) % 2,
           minSpreadBps: min 10000.0 * (ask - bid) % (bid + ask) % 2,
           maxSpreadBps: max 10000.0 * (ask - bid) % (bid + ask) % 2,
           quotes: count i
    by sym
    from quotes
    where date within (startDate; endDate), bid > 0, ask > bid
    }

/ ==============================================================================
/ 4. EXECUTION QUALITY REPORT
/ ==============================================================================

/ Per-symbol execution quality summary
execQuality: {[startDate; endDate]
    enriched: enrichTrades[startDate; endDate];

    select totalTrades: count i,
           totalVolume: sum size,
           avgSlippageBps: avg slippageBps,
           medSlippageBps: med slippageBps,
           pctCrossed: 100.0 * avg crossedSpread,
           avgSpreadBps: avg spreadBps,
           / Cost: total slippage * volume (dollar impact)
           totalCost: sum slippage * size
    by sym
    from enriched
    where not null bid   / only where we have quote data
    }

/ Execution quality by side (buy vs sell)
execBySide: {[startDate; endDate]
    enriched: enrichTrades[startDate; endDate];
    select trades: count i, avgSlippageBps: avg slippageBps,
           avgSpreadBps: avg spreadBps
    by sym, side
    from enriched
    where not null bid
    }

/ ==============================================================================
/ 5. MARKET IMPACT
/ ==============================================================================
/ How much did our trade move the market?
/ Compare price N seconds after trade vs at time of trade

marketImpact: {[s; d; lookForwardSecs]
    t: select sym, time, price, size, side from trades where sym = s, date = d;
    q: select sym, time, bid, ask from quotes where sym = s, date = d;

    / Get quote at time of trade (asof join)
    atTrade: aj[`sym`time; t; q];

    / Get quote lookForwardSecs after trade (shift the time window)
    tShifted: update time: time + `time$1000 * lookForwardSecs from t;
    afterTrade: aj[`sym`time; tShifted; q];

    / Calculate impact
    atTrade: update
        midAtTrade: (bid + ask) % 2.0
    from atTrade;

    afterTrade: update
        midAfter: (bid + ask) % 2.0
    from afterTrade;

    / Join results
    result: atTrade ,' (select midAfter from afterTrade);
    result: update
        impact: ?[side = `buy;
            midAfter - midAtTrade;       / buy: market moved up = our impact
            midAtTrade - midAfter],      / sell: market moved down
        impactBps: 10000.0 * ?[side = `buy;
            (midAfter - midAtTrade) % midAtTrade;
            (midAtTrade - midAfter) % midAtTrade]
    from result;

    result
    }

/ ==============================================================================
/ 6. IMPLEMENTATION SHORTFALL
/ ==============================================================================
/ Compares actual execution vs decision price (e.g., arrival price)
/ IS = (Execution Price - Decision Price) / Decision Price * side_sign

implementationShortfall: {[s; d]
    t: select sym, time, price, size, side from trades where sym = s, date = d;

    / Decision price = first mid of the day (arrival price benchmark)
    q: select sym, time, bid, ask from quotes where sym = s, date = d;
    arrivalMid: exec first (bid + ask) % 2.0 from q;

    / Calculate IS for each trade
    t: update
        arrivalPx: arrivalMid,
        isBps: 10000.0 * ?[side = `buy;
            (price - arrivalMid) % arrivalMid;
            (arrivalMid - price) % arrivalMid]
    from t;

    / Summary
    show "Implementation Shortfall for ", (string s), " on ", string d;
    show "Arrival price (first mid): ", string arrivalMid;
    show select avgISbps: avg isBps, totalTrades: count i, totalVol: sum size by side from t;
    t
    }

/ ==============================================================================
/ 7. FULL TCA REPORT
/ ==============================================================================

runTCA: {[s; startDate; endDate]
    reportHeader "TCA Report: ", (string s), " (", (string startDate), " to ", (string endDate), ")";

    / Execution quality
    show "=== Execution Quality ===";
    eq: execQuality[startDate; endDate];
    show select from eq where sym = s;

    / Spread stats
    show "";
    show "=== Spread Statistics ===";
    spStats: spreadStats[startDate; endDate];
    show select from spStats where sym = s;

    / Volume profile
    show "";
    show "=== Volume by Time of Day ===";
    vp: select vol: sum size by bucket: 60 xbar time.minute from trades where sym = s, date within (startDate; endDate);
    show vp;

    show "";
    show "TCA complete";
    }

show "tca.q loaded - Transaction Cost Analysis ready";
show "  enrichTrades[sd; ed]                  - Asof join trades to quotes";
show "  execQuality[sd; ed]                   - Execution quality report";
show "  spreadStats[sd; ed]                   - Spread statistics";
show "  marketImpact[`AAPL; date; 30]         - 30-sec market impact";
show "  implementationShortfall[`AAPL; date]  - IS analysis";
show "  runTCA[`AAPL; sd; ed]                 - Full TCA report";
