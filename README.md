# Blockchain Analyzer

This repository contains a proof of concept for a blockchain monitoring tool.

## Configuration

The application expects a configuration file at `blockchain_analyzer/module_config.yaml`. An example file is provided with default values that should be adjusted for your environment (RPC endpoints, API keys and alert settings).

```
blockchains:
  ethereum:
    rpc_endpoints:
      - https://mainnet.infura.io/v3/YOUR_API_KEY
    scan_api: https://api.etherscan.io/api
    scan_api_key: YOUR_ETHERSCAN_KEY
    chain_id: 1
```

See the file for all available options.

## Dashboard assets

The FastAPI dashboard uses Jinja templates and static files:

* `templates/` – HTML templates for the web interface. A minimal `dashboard.html` is included so the server can start.
* `static/` – static resources such as CSS or JavaScript files. A basic stylesheet is provided.

These directories must exist even if they only contain placeholder files; otherwise FastAPI will raise an error during startup.

## Running

To start the web dashboard:

```
python BlockchainAnalyzer.py
```

This will launch a server on port 8000 and load the configuration from `blockchain_analyzer/module_config.yaml`.
