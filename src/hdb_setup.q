/ ==============================================================================
/ hdb_setup.q - Historical Database Setup & Management
/ ==============================================================================
/ Concepts: partitioned tables, splayed tables, sym file, .Q.en enumeration,
/           on-disk storage, .d column order file, memory mapping
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. DATABASE DIRECTORY STRUCTURE
/ ==============================================================================
/ A partitioned HDB looks like this on disk:
/
/   db/                        <- database root
/     sym                      <- master symbol enumeration file
/     2024.01.02/              <- one directory per trading date
/       trades/                <- table name = directory
/         sym                  <- column file (integers, enumerated)
/         time                 <- column file (binary times)
/         price                <- column file (binary floats)
/         size                 <- column file (binary longs)
/         side                 <- column file (enumerated symbols)
/         .d                   <- column order metadata
/       quotes/                <- another table
/         sym, time, bid, ask, bidSize, askSize, .d
/     2024.01.03/
/       trades/
/         ...
/     2024.01.04/
/       trades/
/         ...

dbPath: `:db;

/ ==============================================================================
/ 2. CREATING A PARTITIONED DATABASE
/ ==============================================================================

/ --- savePartition: Save one day of data to disk ---
/ This is the core function used during EOD writedown
/ CRITICAL: Must enumerate symbols with .Q.en BEFORE saving

savePartition: {[dbDir; dt; tableName; data]
    / Step 1: Enumerate symbol columns (convert symbols to integers)
    / .Q.en looks up each symbol in the master sym file at dbDir/sym
    / If a new symbol is found, it APPENDS it to the sym file
    enumData: .Q.en[dbDir; data];

    / Step 2: Build the save path: db/2024.01.15/trades/
    savePath: ` sv dbDir, (`$string dt), tableName, `;

    / Step 3: Write to disk (each column becomes a binary file)
    savePath set enumData;

    logInfo "Saved ", (string count data), " rows to ", string savePath;
    }

/ --- buildHDB: Build entire partitioned database from raw data ---
buildHDB: {[dbDir; rawTrades; rawQuotes]
    reportHeader "Building Partitioned HDB";

    / Get all unique dates
    tradeDates: asc distinct rawTrades`date;
    logInfo "Processing ", (string count tradeDates), " trading days";

    / Save trades partition for each date
    {[dbDir; rawTrades; d]
        dayTrades: select sym, time, price, size, side, exch, cond
                   from rawTrades where date = d;
        if[0 < count dayTrades;
            / Sort by sym then time (required for partitioned tables)
            dayTrades: `sym`time xasc dayTrades;
            savePartition[dbDir; d; `trades; dayTrades];
        ];
    }[dbDir; rawTrades;] each tradeDates;

    / Save quotes partition for each date
    quoteDates: asc distinct rawQuotes`date;
    {[dbDir; rawQuotes; d]
        dayQuotes: select sym, time, bid, ask, bidSize, askSize
                   from rawQuotes where date = d;
        if[0 < count dayQuotes;
            dayQuotes: `sym`time xasc dayQuotes;
            savePartition[dbDir; d; `quotes; dayQuotes];
        ];
    }[dbDir; rawQuotes;] each quoteDates;

    logInfo "HDB build complete";
    }

/ ==============================================================================
/ 3. LOADING THE HDB
/ ==============================================================================

loadHDB: {[dbDir]
    logInfo "Loading HDB from ", string dbDir;
    / \l loads a database - Q maps the partitioned files into memory
    system "l ", 1 _ string dbDir;
    logInfo "HDB loaded. Tables: ", " " sv string tables[];
    logInfo "Partitions: ", string count date;  / 'date' is auto-created as the partition column
    }

/ ==============================================================================
/ 4. HDB MAINTENANCE FUNCTIONS
/ ==============================================================================

/ --- addPartition: Add a new date to existing HDB ---
addPartition: {[dbDir; dt; tableName; data]
    savePartition[dbDir; dt; tableName; `sym`time xasc data];
    / Reload to pick up new partition
    system "l ", 1 _ string dbDir;
    logInfo "Added partition ", string dt;
    }

/ --- getPartitions: List all date partitions ---
getPartitions: {[dbDir]
    / Read directory listing, parse as dates
    dirs: key dbDir;
    dates: "D"$string dirs;
    dates where not null dates
    }

/ --- inspectPartition: Show details of one partition ---
inspectPartition: {[dbDir; dt; tableName]
    reportHeader "Partition: ", (string dt), " / ", string tableName;
    partPath: ` sv dbDir, (`$string dt), tableName;
    show "Path:    ", string partPath;
    show "Columns: ", " " sv string key partPath;

    / Read .d file (column order)
    dFilePath: ` sv partPath, `.d;
    show "Column order (.d): ";
    show get dFilePath;

    / Show row count and sample
    t: get partPath;
    show "Rows:    ", string count t;
    show "Sample:  ";
    show 5 # t;
    }

/ ==============================================================================
/ 5. SYM FILE OPERATIONS
/ ==============================================================================

/ --- inspectSymFile: Show the master symbol enumeration ---
inspectSymFile: {[dbDir]
    symPath: ` sv dbDir, `sym;
    syms: get symPath;
    show "Sym file: ", string symPath;
    show "Total symbols: ", string count syms;
    show "Symbols: ", " " sv string syms;
    }

/ --- addSymbols: Manually add new symbols to the sym file ---
addSymbols: {[dbDir; newSyms]
    symPath: ` sv dbDir, `sym;
    existing: get symPath;
    toAdd: newSyms except existing;
    if[0 < count toAdd;
        symPath set existing, toAdd;
        logInfo "Added ", (string count toAdd), " new symbols: ", " " sv string toAdd;
    ];
    }

/ ==============================================================================
/ 6. VALIDATION
/ ==============================================================================

validateHDB: {[dbDir]
    reportHeader "HDB Validation";

    / Check sym file exists
    symPath: ` sv dbDir, `sym;
    show "Sym file: ", $[() ~ key symPath; "MISSING!"; "OK (", (string count get symPath), " symbols)"];

    / Check partitions
    parts: getPartitions[dbDir];
    show "Partitions: ", string count parts;
    show "Date range: ", (string min parts), " to ", string max parts;

    / Check each table
    {[dbDir; p]
        tables: key ` sv dbDir, `$string p;
        show "  ", (string p), ": ", " " sv string tables;
    }[dbDir;] each 3 # parts;  / Check first 3 partitions

    show "Validation complete";
    }

show "hdb_setup.q loaded - HDB management functions ready";
show "  buildHDB[dbPath; trades; quotes]   - Build full HDB from data";
show "  loadHDB[dbPath]                    - Load HDB into memory";
show "  inspectPartition[dbPath; date; tab] - Inspect a partition";
show "  inspectSymFile[dbPath]             - Show sym file contents";
show "  validateHDB[dbPath]               - Run validation checks";
