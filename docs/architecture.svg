<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 900 560" font-family="-apple-system, Segoe UI, sans-serif">
  <style>
    .box { fill: #f5f7fa; stroke: #2a4d8f; stroke-width: 1.5; rx: 8; }
    .layer1 { fill: #e8f0fb; }
    .layer2 { fill: #fbf0e8; }
    .layer3 { fill: #ecf6ec; }
    .ext { fill: #f5f5f5; stroke: #888; stroke-dasharray: 4 3; }
    .s3 { fill: #fff7e0; stroke: #c89010; stroke-width: 1.5; rx: 8; }
    .lbl { font-size: 13px; fill: #1a1a1a; font-weight: 600; }
    .sub { font-size: 11px; fill: #555; }
    .layerlbl { font-size: 12px; fill: #2a4d8f; font-weight: 700; letter-spacing: 0.5px; }
    .arrow { fill: none; stroke: #444; stroke-width: 1.5; marker-end: url(#ah); }
    .arrowtxt { font-size: 10px; fill: #666; font-style: italic; }
  </style>
  <defs>
    <marker id="ah" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L0,6 L9,3 z" fill="#444"/>
    </marker>
  </defs>

  <text x="450" y="28" text-anchor="middle" font-size="16" font-weight="700" fill="#1a1a1a">
    Wikipedia Attention Pipeline — Architecture
  </text>

  <!-- External sources -->
  <rect x="40" y="65" width="170" height="70" class="ext" rx="8"/>
  <text x="125" y="92" class="lbl" text-anchor="middle">Wikimedia dumps</text>
  <text x="125" y="110" class="sub" text-anchor="middle">~65,000 hourly .gz files</text>
  <text x="125" y="125" class="sub" text-anchor="middle">~3 TB compressed</text>

  <rect x="690" y="65" width="170" height="70" class="ext" rx="8"/>
  <text x="775" y="92" class="lbl" text-anchor="middle">FRED / BLS API</text>
  <text x="775" y="110" class="sub" text-anchor="middle">UNRATE, ICSA, PAYEMS</text>
  <text x="775" y="125" class="sub" text-anchor="middle">monthly + weekly series</text>

  <!-- Layer 1 -->
  <text x="30" y="180" class="layerlbl">LAYER 1 — INGEST</text>
  <rect x="40" y="195" width="380" height="90" class="box layer1"/>
  <text x="230" y="222" class="lbl" text-anchor="middle">AWS Lambda · pageview-ingest</text>
  <text x="230" y="244" class="sub" text-anchor="middle">Python 3.11, 1024 MB, 5 min timeout</text>
  <text x="230" y="262" class="sub" text-anchor="middle">~40 concurrent invocations · ~80 min wall-clock</text>
  <text x="230" y="278" class="sub" text-anchor="middle">stream gunzip → filter ~30 titles → write Parquet</text>

  <rect x="450" y="195" width="240" height="90" class="box layer1"/>
  <text x="570" y="222" class="lbl" text-anchor="middle">orchestrator.py</text>
  <text x="570" y="244" class="sub" text-anchor="middle">generate (y,m,d,h) payloads</text>
  <text x="570" y="262" class="sub" text-anchor="middle">async Lambda invoke (boto3)</text>
  <text x="570" y="278" class="sub" text-anchor="middle">S3-list idempotent retry rounds</text>

  <!-- S3 staging -->
  <rect x="180" y="305" width="540" height="48" class="s3"/>
  <text x="450" y="328" class="lbl" text-anchor="middle">S3 · staging/year=YYYY/month=MM/day=DD/hour=HH.jsonl.gz</text>
  <text x="450" y="345" class="sub" text-anchor="middle">~65,000 tiny gzipped JSONL files · ~60 MB total</text>

  <!-- Layer 2 -->
  <text x="30" y="385" class="layerlbl">LAYER 2 — AGGREGATE</text>
  <rect x="40" y="400" width="380" height="80" class="box layer2"/>
  <text x="230" y="427" class="lbl" text-anchor="middle">PySpark · aggregate_pageviews.py</text>
  <text x="230" y="447" class="sub" text-anchor="middle">EC2 m5.2xlarge, local[*], ~10 min</text>
  <text x="230" y="463" class="sub" text-anchor="middle">redirect resolution (broadcast join)</text>
  <text x="230" y="478" class="sub" text-anchor="middle">daily / weekly / monthly rollups</text>

  <rect x="450" y="400" width="240" height="80" class="s3"/>
  <text x="570" y="430" class="lbl" text-anchor="middle">S3 · curated/</text>
  <text x="570" y="450" class="sub" text-anchor="middle">{daily,weekly,monthly}/year=YYYY/</text>
  <text x="570" y="466" class="sub" text-anchor="middle">partitioned for Athena pruning</text>

  <!-- Layer 3 -->
  <text x="30" y="510" class="layerlbl">LAYER 3 — ANALYZE</text>
  <rect x="40" y="520" width="280" height="35" class="box layer3"/>
  <text x="180" y="543" class="lbl" text-anchor="middle">Athena + Glue Catalog (SQL on S3)</text>

  <rect x="340" y="520" width="280" height="35" class="box layer3"/>
  <text x="480" y="543" class="lbl" text-anchor="middle">analyze.py — Granger + changepoint</text>

  <rect x="640" y="520" width="220" height="35" class="box layer3"/>
  <text x="750" y="543" class="lbl" text-anchor="middle">figures/, *.csv → README</text>

  <!-- Arrows -->
  <path class="arrow" d="M125,135 L125,195"/>
  <text x="135" y="170" class="arrowtxt">HTTPS GET</text>

  <path class="arrow" d="M570,285 L450,305"/>
  <path class="arrow" d="M230,285 L300,305"/>
  <path class="arrow" d="M450,353 L230,400"/>
  <path class="arrow" d="M450,480 L450,520"/>
  <path class="arrow" d="M620,480 L320,520"/>
  <path class="arrow" d="M775,135 L620,520"/>
  <text x="760" y="350" class="arrowtxt" transform="rotate(70 760 350)">macro indicators</text>
</svg>
