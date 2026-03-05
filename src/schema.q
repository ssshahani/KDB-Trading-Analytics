/ ==============================================================================
/ schema.q - Table Schemas & Reference Data
/ ==============================================================================
/ Defines all table structures used across the trading analytics platform.
/ Concepts: atoms, lists, dictionaries, tables, keyed tables, data types
/ ==============================================================================

/ ==============================================================================
/ TRADE TABLE - Core tick data from exchange
/ ==============================================================================
tradeSchema: ([]
    sym:       `symbol$();      / Stock ticker (enumerated)
    date:      `date$();        / Trade date
    time:      `time$();        / Execution time (ms precision)
    price:     `float$();       / Execution price
    size:      `long$();        / Shares traded
    side:      `symbol$();      / `buy or `sell
    exch:      `symbol$();      / Exchange: `NYSE`NASDAQ`ARCA`BATS
    cond:      `symbol$()      / Condition: `reg`odd`block
    )

/ ==============================================================================
/ QUOTE TABLE - Best bid/offer (BBO)
/ ==============================================================================
quoteSchema: ([]
    sym:       `symbol$();
    date:      `date$();
    time:      `time$();
    bid:       `float$();       / Best bid price
    ask:       `float$();       / Best ask price
    bidSize:   `long$();        / Shares at bid
    askSize:   `long$();        / Shares at ask
    exch:      `symbol$()
    )

/ ==============================================================================
/ ORDER TABLE - Order lifecycle tracking
/ ==============================================================================
orderSchema: ([]
    orderId:   `symbol$();      / Unique order ID
    sym:       `symbol$();
    date:      `date$();
    submitTime:`time$();        / When submitted
    fillTime:  `time$();        / When filled (0Nt if unfilled)
    side:      `symbol$();
    orderType: `symbol$();      / `limit`market`stop`vwap
    limitPx:   `float$();       / Limit price (0n for market)
    fillPx:    `float$();       / Fill price
    qty:       `long$();        / Ordered quantity
    fillQty:   `long$();        / Filled quantity
    status:    `symbol$();      / `new`partial`filled`cancelled
    algo:      `symbol$()       / `DMA`TWAP`VWAP`IS
    )

/ ==============================================================================
/ DAILY OHLCV TABLE - Aggregated daily bars
/ ==============================================================================
dailySchema: ([]
    sym:    `symbol$();
    date:   `date$();
    open:   `float$();
    high:   `float$();
    low:    `float$();
    close:  `float$();
    volume: `long$()
    )

/ ==============================================================================
/ REFERENCE DATA - Keyed tables for joins
/ ==============================================================================
/ Keyed tables: columns inside [] form the primary key
/ Used with lj (left join) for enrichment

refData: ([sym: `AAPL`MSFT`GOOG`AMZN`NVDA`META`TSLA`JPM`GS`BAC`JNJ`PFE`UNH`XOM`CVX`COP`WMT`KO`PG`BA]
    name:     ("Apple";"Microsoft";"Alphabet";"Amazon";"NVIDIA";"Meta";"Tesla";"JP Morgan";"Goldman Sachs";"Bank of America";"Johnson & Johnson";"Pfizer";"UnitedHealth";"Exxon Mobil";"Chevron";"ConocoPhillips";"Walmart";"Coca-Cola";"Procter & Gamble";"Boeing");
    sector:   `Tech`Tech`Tech`Tech`Tech`Tech`Tech`Finance`Finance`Finance`Healthcare`Healthcare`Healthcare`Energy`Energy`Energy`Consumer`Consumer`Consumer`Industrial;
    mktCap:   3.0 2.8 1.9 1.8 3.2 1.2 0.8 0.55 0.15 0.28 0.38 0.16 0.52 0.45 0.30 0.13 0.55 0.27 0.38 0.12;
    currency: 20#`USD
    )

/ --- Exchange Reference ---
exchRef: ([exch: `NYSE`NASDAQ`ARCA`BATS`IEX]
    name:     ("New York Stock Exchange";"NASDAQ";"NYSE Arca";"BATS Global";"IEX");
    openTime: 5#09:30:00.000;
    closeTime:5#16:00:00.000
    )

/ --- Sector Benchmark Weights ---
sectorWeights: ([sector: `Tech`Finance`Healthcare`Energy`Consumer`Industrial]
    benchWeight: 0.35 0.15 0.15 0.12 0.13 0.10;
    riskBucket:  `high`medium`medium`high`low`medium
    )

/ ==============================================================================
/ CONFIGURATION DICTIONARY
/ ==============================================================================
config: `dbPath`logPath`numSyms`startDate`endDate ! (`:db; `:logs; 20; 2022.01.03; 2024.12.31)

/ ==============================================================================
/ HELPER FUNCTIONS
/ ==============================================================================
showSchema: {[]
    show "=== Table Schemas ===";
    show "trades:  "; show meta tradeSchema;
    show "quotes:  "; show meta quoteSchema;
    show "orders:  "; show meta orderSchema;
    show "";
    show "=== Reference Data ===";
    show "Symbols: ", string count refData;
    show "Sectors: ", " " sv string exec distinct sector from refData;
    }

show "schema.q loaded - table schemas and reference data ready";
