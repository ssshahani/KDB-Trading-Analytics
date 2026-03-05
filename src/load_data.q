/ ==============================================================================
/ load_data.q - Load Market Data & Build HDB (Lightweight Version)
/ ==============================================================================
/ Usage: q src/load_data.q
/ ==============================================================================

\l src/schema.q
\l src/utils.q

dbDir: `:db;

/ ==============================================================================
/ 1. LOAD DAILY DATA FROM CSV
/ ==============================================================================

-1 "=== Loading Market Data ===";
-1 "Loading daily OHLCV from CSV...";

/ Column types: S=symbol, D=date, F=float(x4), J=long
dailyRaw: ("SDFFFFJ"; enlist ",") 0: `:data/raw/all_daily.csv;
-1 "Loaded ", (string count dailyRaw), " daily rows";

/ Store as daily OHLCV table
daily: `sym`date xasc dailyRaw;
-1 "Date range: ", (string min daily`date), " to ", string max daily`date;
-1 "Symbols: ", string count distinct daily`sym;
show 5 # daily;

/ ==============================================================================
/ 2. GENERATE SIMULATED INTRADAY TICK DATA
/ ==============================================================================
/ We generate a SMALL number of ticks per day to keep memory low.
/ On the free 32-bit edition (4GB limit), we must be conservative.

-1 "";
-1 "=== Generating Intraday Ticks ===";
-1 "Using lightweight mode (fewer ticks per day for 32-bit compatibility)";

/ For each day + symbol, generate N trades and M quotes
genDayTicks: {[row]
    s: row`sym;
    d: row`date;
    o: row`open; h: row`high; l: row`low; c: row`close; v: row`volume;

    / Keep it small: 20 trades, 40 quotes per symbol per day
    nTrades: 20;
    nQuotes: 40;

    / Generate random times within trading hours (sorted)
    tradeTimes: asc nTrades ? 09:30:00.000 + til `long$6.5*3600000;
    quoteTimes: asc nQuotes ? 09:30:00.000 + til `long$6.5*3600000;

    / Prices within high/low range
    priceRange: 0.01 | h - l;
    tradePrices: l + nTrades ? priceRange;
    tradePrices[0]: o;
    tradePrices[nTrades - 1]: c;

    / Sizes
    tradeSizes: 100 * 1 + nTrades ? 10;

    / Build trade table
    dayTrades: ([]
        sym:   nTrades # s;
        date:  nTrades # d;
        time:  tradeTimes;
        price: tradePrices;
        size:  tradeSizes;
        side:  nTrades ? `buy`sell;
        exch:  nTrades ? `NYSE`NASDAQ`ARCA`BATS;
        cond:  nTrades ? `reg`reg`reg`reg`odd
    );

    / Build quote table
    midPrices: l + nQuotes ? priceRange;
    spread: 0.005 + nQuotes ? 0.01;
    dayQuotes: ([]
        sym:     nQuotes # s;
        date:    nQuotes # d;
        time:    quoteTimes;
        bid:     midPrices - spread % 2;
        ask:     midPrices + spread % 2;
        bidSize: 100 * 1 + nQuotes ? 20;
        askSize: 100 * 1 + nQuotes ? 20;
        exch:    nQuotes ? `NYSE`NASDAQ`ARCA`BATS
    );

    (dayTrades; dayQuotes)
    }

/ Process only last 6 months to keep it manageable
cutoffDate: max[daily`date] - 180;
recentDaily: select from daily where date >= cutoffDate;
allDates: asc distinct recentDaily`date;
-1 "Processing ", (string count allDates), " recent trading days (last 6 months)";

/ Process each date
allTrades: tradeSchema;
allQuotes: quoteSchema;

cnt: 0;
{[d]
    dayRows: select from recentDaily where date = d;
    results: genDayTicks each dayRows;
    `allTrades insert raze results[;0];
    `allQuotes insert raze results[;1];
    cnt +: 1;
    if[0 = cnt mod 30;
        -1 "  Processed ", (string cnt), " / ", (string count allDates), " days";
    ];
    } each allDates;

-1 "Trades generated: ", string count allTrades;
-1 "Quotes generated: ", string count allQuotes;

/ ==============================================================================
/ 3. BUILD PARTITIONED HDB
/ ==============================================================================

-1 "";
-1 "=== Building Partitioned HDB ===";

cnt2: 0;
{[d]
    dayTrades: `sym`time xasc select sym, time, price, size, side, exch, cond from allTrades where date = d;
    dayQuotes: `sym`time xasc select sym, time, bid, ask, bidSize, askSize from allQuotes where date = d;

    if[0 < count dayTrades;
        (` sv dbDir, (`$string d), `trades, `) set .Q.en[dbDir; dayTrades];
    ];
    if[0 < count dayQuotes;
        (` sv dbDir, (`$string d), `quotes, `) set .Q.en[dbDir; dayQuotes];
    ];

    cnt2 +: 1;
    if[0 = cnt2 mod 30;
        -1 "  Saved ", (string cnt2), " / ", (string count allDates), " partitions";
    ];
    } each allDates;

-1 "All partitions saved";

/ ==============================================================================
/ 4. SAVE DAILY TABLE
/ ==============================================================================

`:db/daily set daily;
-1 "Daily OHLCV table saved (full 3 years)";

/ ==============================================================================
/ 5. LOAD AND VERIFY
/ ==============================================================================

-1 "";
-1 "=== Loading & Verifying HDB ===";
system "l db";

-1 "Tables:     ", " " sv string tables[];
-1 "Trade rows: ", string count trades;
-1 "Quote rows: ", string count quotes;
-1 "Daily rows: ", string count daily;
-1 "";
-1 "Sample trades:";
show 5 # select from trades where date within (min date; min date);
-1 "";
-1 "Sample quotes:";
show 5 # select from quotes where date within (min date; min date);

-1 "";
-1 "========================================";
-1 "  DATABASE READY!";
-1 "========================================";
-1 "";
-1 "Next steps:";
-1 "  \\l src/analytics.q";
-1 "  \\l src/tca.q";
-1 "  \\l src/risk.q";
-1 "  \\l src/alerts.q";
-1 "  \\l src/reports.q";
