#!/bin/bash
cd ~/kdb-trading-analytics/db
q <<'EOF'
\l .
\l /home/ssuser/kdb-trading-analytics/src/schema.q
\l /home/ssuser/kdb-trading-analytics/src/utils.q
\l /home/ssuser/kdb-trading-analytics/src/analytics.q
\l /home/ssuser/kdb-trading-analytics/src/tca.q
\l /home/ssuser/kdb-trading-analytics/src/risk.q
\l /home/ssuser/kdb-trading-analytics/src/alerts.q
\l /home/ssuser/kdb-trading-analytics/src/reports.q
\l /home/ssuser/kdb-trading-analytics/tests/test_analytics.q
EOF
