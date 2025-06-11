# Blockchain Analyzer

O Blockchain Analyzer é um proof of concept para monitorar atividades de
blockchain e gerar insights de risco. Ele fornece rastreamento de tokens e
carteiras, monitoramento do mempool em tempo real e um painel simples para
visualizar os dados coletados.

## Funcionalidades

* Suporta múltiplas blockchains via endpoints RPC configuráveis
* Pontuação de risco para tokens e carteiras utilizando aprendizado de máquina
* Listas de observação para endereços ou contratos
* Alertas via Discord, Telegram ou e‑mail
* Painel web baseado em FastAPI
* Funciona totalmente via RPC/Web3 sem a necessidade de APIs externas de scan

## Instalação

1. Clone este repositório.
2. Crie um ambiente Python (opcional, mas recomendado).
3. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração

Todas as configurações ficam em `blockchain_analyzer/module_config.yaml`. Atualize os endpoints RPC e canais de alerta para seu ambiente. Abaixo está um exemplo resumido:

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

Consulte o arquivo para todas as opções disponíveis.

## Execução

Execute o painel web com:

```bash
python -m blockchain_analyzer.webapp
```

O `main.py` fornecido chama este módulo por conveniência e também pode ser usado
para iniciar a aplicação.

O painel ficará disponível em `http://localhost:8000/` e carrega as configurações de `blockchain_analyzer/module_config.yaml`.

## Atualizando a lista de golpistas

O arquivo `data/multichain.json` contém endereços de golpistas conhecidos
utilizados pelo rastreador. Para atualizá-lo com a versão mais recente do
repositório original execute:

```bash
python scripts/update_scammers_list.py
```

Esta etapa é opcional e requer acesso à internet.

## Licença

Este projeto está licenciado sob a [Licença MIT](LICENSE).
