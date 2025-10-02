# Cloudflare Edge Probes

These probes run on Cloudflare's global edge network (Workers) to simulate real-world user traffic hitting the Edge Observatory Gateway from various regions simultaneously.

## Deployment

You will need a free Cloudflare account and `wrangler` installed.

1. Install Wrangler CLI:
```bash
npm install -g wrangler
```

2. Login to Cloudflare:
```bash
wrangler login
```

3. Initialize a new worker project in this directory (if not done already):
```bash
wrangler init EdgeObservatoryProbes
```

4. Ensure your `wrangler.toml` looks like this:
```toml
name = "edge-observatory-probes"
main = "edge-worker.js"
compatibility_date = "2024-03-07"

# Run this worker every 1 minute
[triggers]
crons = ["* * * * *"]

[vars]
# Change this to your production Gateway URL when DNS is live
GATEWAY_URL = "https://chaos.jeshwanth.dev/telemetry"
```

5. Deploy the worker to the edge:
```bash
wrangler deploy
```

## How It Works

1. The Cron trigger fires every 1 minute across multiple Cloudflare edge colos.
2. The worker script measures execution latency and grabs its current geographical region (`CF_SJC`, `CF_IAD`, etc).
3. It POSTs a JSON payload to the Gateway's `/telemetry` endpoint.
4. The Go Gateway marshals the JSON into protobuf and ingests it into the Kafka pipeline.
