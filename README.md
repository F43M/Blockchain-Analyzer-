# Blockchain Analyzer

Blockchain Analyzer is a proof of concept for monitoring blockchain activity and generating risk insights. It provides token and wallet tracking, real time mempool monitoring and a simple dashboard to visualize the collected data.

## Features

* Supports multiple blockchains via configurable RPC endpoints
* Risk scoring for tokens and wallets using machine learning
* Watchlists for addresses or contracts
* Alerts through Discord, Telegram or email
* FastAPI based web dashboard
* Works entirely via RPC/Web3 without the need for external scan APIs

## Installation

1. Clone this repository.
2. Create a Python environment (optional but recommended).
3. Install the dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

All settings live in `blockchain_analyzer/module_config.yaml`. Update RPC endpoints and alert channels to match your environment. Below is a shortened example:

```yaml
blockchains:
  ethereum:
    rpc_endpoints:
      - "https://mainnet.infura.io/v3/YOUR_API_KEY"
    chain_id: 1
alerts:
  discord_webhook: ""
  telegram_bot_token: ""
  telegram_chat_id: ""
risk_thresholds:
  whale_threshold_eth: 10.0
machine_learning:
  model_path: "ml_models/behavior_model.joblib"
```

See the file for all available options.

## Running

Start the application with:

```bash
python main.py
```

The dashboard will be available at `http://localhost:8000/` and loads the configuration from `blockchain_analyzer/module_config.yaml`.

## License

This project is licensed under the [MIT License](LICENSE).
