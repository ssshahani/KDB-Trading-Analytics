/ ==============================================================================
/ alerts.q - Anomaly Detection & Alert Engine
/ ==============================================================================
/ Concepts: conditional logic, where clauses, mavg for baselines,
/           table operations, alert aggregation
/ ==============================================================================

// \l src/schema.q
// \l src/utils.q

/ ==============================================================================
/ 1. VOLUME ANOMALY DETECTION
/ ==============================================================================
/ Flag days where volume > N standard deviations above the moving average

volumeAlerts: {[nDays; threshold]
    / Calculate rolling average and stdev of volume
    t: update avgVol: mavg[nDays; volume],
              stdVol: mdev[nDays; volume]
       by sym from daily;

    / Flag anomalies: volume > avg + threshold * stdev
    t: update volZscore: (volume - avgVol) % stdVol from t;

    alerts: select sym, date, volume, avgVol, stdVol, volZscore
            from t
            where volZscore > threshold, not null avgVol;

    alerts: update alertType: `volumeSpike,
                   severity: ?[volZscore > 4.0; `critical;
                              ?[volZscore > 3.0; `high; `medium]]
            from alerts;

    `date xdesc alerts
    }

/ ==============================================================================
/ 2. PRICE GAP DETECTION
/ ==============================================================================
/ Find days where the open is significantly different from prior close

priceGapAlerts: {[gapThresholdPct]
    / Get previous close via prev function
    t: update prevClose: prev close by sym from daily;
    t: update gapPct: 100.0 * (open - prevClose) % prevClose from t;

    alerts: select sym, date, open, prevClose, gapPct
            from t
            where abs[gapPct] > gapThresholdPct, not null prevClose;

    alerts: update alertType: `priceGap,
                   direction: ?[gapPct > 0; `gapUp; `gapDown],
                   severity: ?[abs[gapPct] > 5.0; `critical;
                              ?[abs[gapPct] > 3.0; `high; `medium]]
            from alerts;

    `date xdesc alerts
    }

/ ==============================================================================
/ 3. VOLATILITY BREAKOUT
/ ==============================================================================
/ Flag when intraday range exceeds N times the recent average range

volatilityAlerts: {[nDays; threshold]
    / Daily range as percentage of close
    t: update range: high - low,
              rangePct: 100.0 * (high - low) % close
       from daily;

    / Rolling average range
    t: update avgRange: mavg[nDays; rangePct] by sym from t;
    t: update rangeRatio: rangePct % avgRange from t;

    alerts: select sym, date, rangePct, avgRange, rangeRatio
            from t
            where rangeRatio > threshold, not null avgRange;

    alerts: update alertType: `volatilityBreakout,
                   severity: ?[rangeRatio > 3.0; `critical;
                              ?[rangeRatio > 2.5; `high; `medium]]
            from alerts;

    `date xdesc alerts
    }

/ ==============================================================================
/ 4. LARGE TRADE DETECTION (from tick data)
/ ==============================================================================
/ Flag individual trades that are unusually large

largeTradeAlerts: {[d; sizeMultiple]
    / Get average trade size per symbol for this date
    avgSizes: select avgSize: avg size by sym from trades where date = d;

    / Find trades above threshold
    t: select sym, time, price, size, side from trades where date = d;
    t: t lj avgSizes;

    alerts: select sym, date: d, time, price, size, side, avgSize,
                   sizeRatio: size % avgSize
            from t
            where size > sizeMultiple * avgSize, not null avgSize;

    alerts: update alertType: `largeTrade,
                   severity: ?[sizeRatio > 10; `critical;
                              ?[sizeRatio > 5; `high; `medium]]
            from alerts;

    `sizeRatio xdesc alerts
    }

/ ==============================================================================
/ 5. SPREAD WIDENING ALERT
/ ==============================================================================
/ Detect when bid-ask spread widens significantly

spreadAlerts: {[d; widthMultiple]
    q: select sym, time, spread: ask - bid from quotes where date = d, bid > 0, ask > bid;

    / Average spread per symbol
    avgSpreads: select avgSpread: avg spread by sym from q;
    q: q lj avgSpreads;

    alerts: select sym, date: d, time, spread, avgSpread,
                   spreadRatio: spread % avgSpread
            from q
            where spread > widthMultiple * avgSpread, not null avgSpread;

    alerts: update alertType: `spreadWidening,
                   severity: ?[spreadRatio > 5; `critical;
                              ?[spreadRatio > 3; `high; `medium]]
            from alerts;

    `spreadRatio xdesc alerts
    }

/ ==============================================================================
/ 6. MASTER ALERT GENERATOR
/ ==============================================================================

generateAlerts: {[]
    reportHeader "Alert Generation";

    / Volume anomalies (20-day avg, >2 sigma)
    va: volumeAlerts[20; 2.0];
    logInfo "Volume alerts: ", string count va;

    / Price gaps (>2%)
    pg: priceGapAlerts[2.0];
    logInfo "Price gap alerts: ", string count pg;

    / Volatility breakouts (20-day avg, >2x normal range)
    vb: volatilityAlerts[20; 2.0];
    logInfo "Volatility alerts: ", string count vb;

    / Combine into master alert table
    masterAlerts: (select sym, date, alertType, severity from va),
                  (select sym, date, alertType, severity from pg),
                  (select sym, date, alertType, severity from vb);

    masterAlerts: `date xdesc masterAlerts;

    show "";
    show "=== Alert Summary ===";
    show "Total alerts: ", string count masterAlerts;
    show "";
    show "By type:";
    show select alerts: count i by alertType from masterAlerts;
    show "";
    show "By severity:";
    show select alerts: count i by severity from masterAlerts;
    show "";
    show "Recent alerts (last 10):";
    show 10 # masterAlerts;

    masterAlerts
    }

/ Alert summary for a specific date
dailyAlertSummary: {[d]
    reportHeader "Daily Alert Summary: ", string d;

    va: select from volumeAlerts[20; 2.0] where date = d;
    pg: select from priceGapAlerts[2.0] where date = d;
    vb: select from volatilityAlerts[20; 2.0] where date = d;

    show "Volume spikes: ", string count va;
    if[0 < count va; show va];
    show "Price gaps: ", string count pg;
    if[0 < count pg; show pg];
    show "Volatility breakouts: ", string count vb;
    if[0 < count vb; show vb];
    }

show "alerts.q loaded - alert engine ready";
show "  generateAlerts[]                 - Run all alert detectors";
show "  volumeAlerts[20; 2.0]            - Volume >2 sigma above 20-day avg";
show "  priceGapAlerts[2.0]              - Price gaps >2%";
show "  volatilityAlerts[20; 2.0]        - Range >2x 20-day average";
show "  dailyAlertSummary[date]          - All alerts for one date";
