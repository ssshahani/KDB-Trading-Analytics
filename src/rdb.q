/ ==============================================================================
/ rdb.q - Real-Time Database (RDB)
/ ==============================================================================
/ Concepts: in-memory tables, subscription, insert, EOD writedown,
/           table upsert, real-time query, recovery from TP log
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. RDB STATE - In-Memory Tables
/ ==============================================================================
/ The RDB holds TODAY's data in RAM for fastest possible queries
/ These tables get cleared at end of day after writedown to HDB

trades: tradeSchema       / Start with empty schema
quotes: quoteSchema
orders: orderSchema

/ RDB metadata
.rdb.date: .z.d           / Current trading date
.rdb.status: `ready       / `ready`active`writingdown

/ ==============================================================================
/ 2. MESSAGE HANDLER - Receives ticks from Tickerplant
/ ==============================================================================
/ In production: .u.upd is called by the TP via IPC
/ The TP sends: neg[rdbHandle] (`.u.upd; `trades; newData)

.rdb.upd: {[tableName; data]
    / Insert new rows into the in-memory table
    / `tableName insert data  - appends rows
    tableName insert data;
    }

/ ==============================================================================
/ 3. SUBSCRIBE TO TICKERPLANT
/ ==============================================================================
/ On startup, RDB subscribes to all tables

.rdb.subscribe: {[]
    logInfo "RDB subscribing to tickerplant...";
    / Register our update handler as a TP subscriber
    .tp.subscribe[`trades; .rdb.upd];
    .tp.subscribe[`quotes; .rdb.upd];
    .tp.subscribe[`orders; .rdb.upd];
    .rdb.status: `active;
    logInfo "RDB subscribed and active";
    }

/ ==============================================================================
/ 4. REAL-TIME QUERIES (what traders/BAs run during the day)
/ ==============================================================================

/ Last price per symbol
.rdb.lastPrices: {[]
    select lastPx: last price, lastTime: last time, totalVol: sum size by sym from trades
    }

/ Real-time VWAP
.rdb.realtimeVWAP: {[]
    select vwap: (sum price * size) % sum size, volume: sum size by sym from trades
    }

/ Current spread per symbol (from latest quote)
.rdb.currentSpread: {[]
    select lastBid: last bid, lastAsk: last ask,
           spread: last[ask] - last bid,
           spreadBps: 10000.0 * (last[ask] - last bid) % (last[bid] + last ask) % 2
    by sym from quotes
    }

/ Intraday high/low
.rdb.intradayRange: {[]
    select high: max price, low: min price, range: (max price) - min price,
           rangePct: 100.0 * ((max price) - min price) % first price
    by sym from trades
    }

/ Volume by 5-minute bucket (for volume profile)
.rdb.volumeProfile: {[s]
    select vol: sum size, trades: count i, avgPx: avg price
    by bucket: 5 xbar time.minute
    from trades where sym = s
    }

/ ==============================================================================
/ 5. END-OF-DAY WRITEDOWN
/ ==============================================================================
/ The critical daily process: write RDB data to HDB disk partitions

.rdb.writedown: {[dbDir]
    .rdb.status: `writingdown;
    reportHeader "EOD WRITEDOWN";
    dt: .rdb.date;

    logInfo "Writing down date: ", string dt;
    logInfo "Trades in memory: ", string count trades;
    logInfo "Quotes in memory: ", string count quotes;

    / Step 1: Sort by sym, time (required for HDB format)
    sortedTrades: `sym`time xasc select sym, time, price, size, side, exch, cond from trades;
    sortedQuotes: `sym`time xasc select sym, time, bid, ask, bidSize, askSize from quotes;

    / Step 2: Enumerate symbols (convert to integers using sym file)
    enumTrades: .Q.en[dbDir; sortedTrades];
    enumQuotes: .Q.en[dbDir; sortedQuotes];

    / Step 3: Save as partitioned splayed tables
    tradePath: ` sv dbDir, (`$string dt), `trades, `;
    quotePath: ` sv dbDir, (`$string dt), `quotes, `;

    tradePath set enumTrades;
    quotePath set enumQuotes;

    logInfo "Written to disk:";
    logInfo "  Trades: ", string tradePath;
    logInfo "  Quotes: ", string quotePath;

    / Step 4: Reload HDB to pick up new partition
    system "l ", 1 _ string dbDir;
    logInfo "HDB reloaded with new partition";

    / Step 5: Clear RDB for next day
    delete from `trades;
    delete from `quotes;
    delete from `orders;

    .rdb.date: .rdb.date + 1;
    .rdb.status: `ready;

    logInfo "RDB cleared. Ready for next trading day: ", string .rdb.date;
    }

/ ==============================================================================
/ 6. RECOVERY FROM TP LOG
/ ==============================================================================
/ If RDB crashes mid-day, rebuild state by replaying the TP log

.rdb.recover: {[logPath]
    logInfo "Starting RDB recovery from TP log...";
    / Clear any partial state
    delete from `trades;
    delete from `quotes;
    delete from `orders;
    / In production: -11!logPath replays each message through .rdb.upd
    / Each log entry is (tableName; data) which calls .rdb.upd[tableName; data]
    logInfo "Recovery complete";
    }

/ ==============================================================================
/ 7. RDB STATUS
/ ==============================================================================

.rdb.status_report: {[]
    reportHeader "RDB Status";
    show "Date:    ", string .rdb.date;
    show "Status:  ", string .rdb.status;
    show "Trades:  ", string count trades;
    show "Quotes:  ", string count quotes;
    show "Orders:  ", string count orders;
    show "Symbols: ", string count exec distinct sym from trades;
    }

show "rdb.q loaded - real-time database ready";
show "  .rdb.subscribe[]         - Subscribe to tickerplant";
show "  .rdb.lastPrices[]        - Get last prices";
show "  .rdb.realtimeVWAP[]      - Real-time VWAP";
show "  .rdb.currentSpread[]     - Current bid/ask spreads";
show "  .rdb.writedown[`:db]     - End-of-day writedown to HDB";
