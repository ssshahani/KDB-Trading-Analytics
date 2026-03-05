/ ==============================================================================
/ tick.q - Tickerplant Simulator
/ ==============================================================================
/ Concepts: IPC (inter-process communication), pub/sub pattern,
/           tickerplant log files, message handling, real-time data flow
/ ==============================================================================
/ In production, the tickerplant is a SEPARATE PROCESS.
/ This file simulates the core logic for learning purposes.
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. TICKERPLANT STATE
/ ==============================================================================
/ The TP maintains: subscriber list, log handle, message count

/ Subscriber list: dictionary of table -> list of callback functions
/ In production these would be IPC handles (connection integers)
.tp.subscribers: (`trades`quotes`orders)!(();();())

/ Message counters
.tp.msgCount: `trades`quotes`orders ! 0 0 0

/ Log file path (TP log is the source of truth for recovery)
.tp.logPath: `:logs/tp_log

/ Whether logging is enabled
.tp.logEnabled: 1b

/ ==============================================================================
/ 2. PUBLISH / SUBSCRIBE
/ ==============================================================================

/ --- Subscribe to a table ---
/ In production: .u.sub[tableName; syms] over IPC
/ The RDB calls this on startup to receive real-time updates

.tp.subscribe: {[tableName; callback]
    if[not tableName in key .tp.subscribers;
        logError "Unknown table: ", string tableName;
        :(::);
    ];
    .tp.subscribers[tableName],: enlist callback;
    logInfo "New subscriber for ", string tableName;
    }

/ --- Publish a message to all subscribers ---
/ This is the core TP function: receive data, log it, fan it out

.tp.publish: {[tableName; data]
    / Step 1: Validate
    if[not tableName in key .tp.subscribers;
        logError "Unknown table for publish: ", string tableName;
        :(::);
    ];

    / Step 2: Write to TP log (critical for recovery)
    if[.tp.logEnabled;
        .tp.logPath set (tableName; data);
    ];

    / Step 3: Increment counter
    .tp.msgCount[tableName] +: count data;

    / Step 4: Fan out to all subscribers
    / In production this would be async IPC: neg[handle] (`.u.upd; tableName; data)
    {[cb; tn; d] cb[tn; d]}[; tableName; data] each .tp.subscribers[tableName];
    }

/ ==============================================================================
/ 3. SIMULATED MARKET DATA GENERATOR
/ ==============================================================================
/ Generates realistic-looking tick data for testing

.tp.genTrade: {[dt; syms; basePrices]
    n: 1;
    sym: n ? syms;
    idx: syms ? first sym;
    basePrice: basePrices idx;
    ([]
        sym:   sym;
        date:  n # dt;
        time:  n ? 09:30:00.000 + til `long$6.5 * 60 * 60 * 1000;   / random time in trading hours
        price: basePrice * 1.0 + (n ? 0.02) - 0.01;                  / +/- 1% random
        size:  100 * 1 + n ? 10;                                     / 100 to 1000 shares
        side:  n ? `buy`sell;
        exch:  n ? `NYSE`NASDAQ`ARCA`BATS;
        cond:  n ? `reg`reg`reg`odd`block                            / mostly regular
    )
    }

.tp.genQuote: {[dt; syms; basePrices]
    n: 1;
    sym: n ? syms;
    idx: syms ? first sym;
    basePrice: basePrices idx;
    mid: basePrice * 1.0 + (n ? 0.02) - 0.01;
    spread: basePrice * 0.0005 + n ? 0.001;      / 5-15 bps spread
    ([]
        sym:     sym;
        date:    n # dt;
        time:    n ? 09:30:00.000 + til `long$6.5 * 60 * 60 * 1000;
        bid:     mid - spread % 2;
        ask:     mid + spread % 2;
        bidSize: 100 * 1 + n ? 20;
        askSize: 100 * 1 + n ? 20;
        exch:    n ? `NYSE`NASDAQ`ARCA`BATS
    )
    }

/ --- simulateDay: Generate and publish a full day of ticks ---
.tp.simulateDay: {[dt; numTrades; numQuotes]
    syms: exec sym from refData;
    basePrices: 150.0 280.0 140.0 175.0 130.0 500.0 250.0 190.0 380.0 35.0 155.0 28.0 520.0 110.0 160.0 120.0 170.0 60.0 165.0 200.0;

    logInfo "Simulating ", (string dt), ": ", (string numTrades), " trades, ", (string numQuotes), " quotes";

    / Generate all trades and quotes for the day
    allTrades: raze .tp.genTrade[dt; syms; basePrices] each til numTrades;
    allQuotes: raze .tp.genQuote[dt; syms; basePrices] each til numQuotes;

    / Sort by time (as they would arrive)
    allTrades: `time xasc allTrades;
    allQuotes: `time xasc allQuotes;

    / Publish
    .tp.publish[`trades; allTrades];
    .tp.publish[`quotes; allQuotes];

    logInfo "Day simulation complete. Trades: ", (string count allTrades), " Quotes: ", string count allQuotes;
    }

/ ==============================================================================
/ 4. TP LOG REPLAY (for recovery)
/ ==============================================================================

.tp.replayLog: {[logPath; callback]
    logInfo "Replaying TP log from ", string logPath;
    / In production: -11!logPath replays the binary log
    / Each entry is (tableName; data) which gets applied via callback
    logInfo "Log replay complete";
    }

/ ==============================================================================
/ 5. TP STATUS
/ ==============================================================================

.tp.status: {[]
    reportHeader "Tickerplant Status";
    show "Message counts:";
    show .tp.msgCount;
    show "";
    show "Subscribers:";
    {show "  ", (string x), ": ", string count .tp.subscribers x} each key .tp.subscribers;
    show "";
    show "Logging: ", $[.tp.logEnabled; "ENABLED"; "DISABLED"];
    }

show "tick.q loaded - tickerplant simulator ready";
show "  .tp.subscribe[`trades; callback]  - Subscribe to trade updates";
show "  .tp.simulateDay[date; nTrades; nQuotes] - Simulate a trading day";
show "  .tp.status[]                      - Show TP status";
