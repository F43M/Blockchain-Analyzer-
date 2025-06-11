#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BlockchainAnalyzer v2.0 - Módulo SaaS Ready
Autor: Fábio Mota
Data: 2025-05-28
Licença: MIT
"""

import os
import time
import json
import asyncio
import logging
import hashlib
import yaml
import aiohttp
import aioredis
import joblib
import numpy as np
import pandas as pd
import requests
from websockets import connect
from typing import Dict, List, Tuple, Optional, Any, Set, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from functools import wraps
from web3 import Web3, AsyncWeb3
from web3.middleware import geth_poa_middleware
from web3.exceptions import TransactionNotFound, BlockNotFound
from web3.types import TxReceipt, BlockData
from collections import defaultdict, deque
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.neighbors import LocalOutlierFactor
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
import smtplib
import telegram
from discord_webhook import DiscordWebhook
import cryptography.fernet
from pydantic import BaseModel, validator, ValidationError
import pytz
from .config import ModuleConfig
from .blockchain import BlockchainConnector, AlertSystem
from .analysis import TokenAnalyzer, DexAnalyzer, AIBehaviorEngine
from .utils import Utils, trace
from .block_scanner import BlockScanner

# ==============================================
# Módulo: MemeCoin Tracker
# ==============================================

class MemeCoinTracker:
    """Rastreador avançado de MemeCoins com análise multi-cadeia"""
    
    def __init__(self, chain: str = 'bsc'):
        self.chain = chain
        self.config = ModuleConfig()
        self.connector = BlockchainConnector(chain)
        self.token_analyzer = TokenAnalyzer(self.connector)
        self.dex_analyzer = DexAnalyzer(self.connector)
        self.ai_engine = AIBehaviorEngine()
        self.block_scanner = BlockScanner(self.connector)
        
        # Caches e armazenamentos
        self.token_cache = {}
        self.holders = defaultdict(dict)
        self.smart_money_wallets = set()
        self.deployers = {}
        self.watched_tokens = set()
        self.watched_wallets = set()
        self.known_scammers = self._load_known_scammers()
        
        # Filas e tarefas em segundo plano
        self.pending_tx_queue = asyncio.Queue()
        self.background_tasks = set()
        
        # Iniciar tarefas em segundo plano
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Inicia tarefas em segundo plano"""
        task = asyncio.create_task(self._process_pending_transactions())
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
        
        task = asyncio.create_task(self._monitor_whale_activity())
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)
    
    async def _process_pending_transactions(self):
        """Processa transações pendentes da fila"""
        while True:
            tx = await self.pending_tx_queue.get()
            try:
                await self._analyze_pending_transaction(tx)
            except Exception as e:
                trace.log_event(
                    "PENDING_TX_ANALYSIS",
                    "ERROR",
                    {
                        "tx_hash": tx['hash'].hex() if 'hash' in tx else 'unknown',
                        "error": str(e)
                    }
                )
    
    async def _analyze_pending_transaction(self, tx: Dict):
        """Analisa uma transação pendente"""
        if not tx or 'to' not in tx or 'from' not in tx:
            return
        
        # Verificar se a transação envolve tokens ou carteiras monitoradas
        if tx['to'] in self.watched_tokens or tx['from'] in self.watched_wallets:
            swap_info = await self.dex_analyzer.analyze_swap(tx['hash'].hex())
            
            for event in swap_info.get('events', []):
                if event['type'] == 'swap':
                    direction = self.dex_analyzer.get_swap_direction(event)
                    amount = max(
                        int(event.get('amount0_in', 0)),
                        int(event.get('amount1_in', 0))
                    )
                    
                    if (direction == 'buy' and tx['from'] in self.watched_wallets and 
                        amount >= self.config.thresholds.whale_threshold_eth * 10**18):
                        
                        token_info = await self.token_analyzer.get_token_info(tx['to'])
                        
                        AlertSystem.send_alert(
                            AlertSystem.format_alert(
                                "WHALE_ACTIVITY",
                                self.chain,
                                {
                                    "address": tx['from'],
                                    "token": tx['to'],
                                    "amount": Utils.wei_to_eth(amount),
                                    "symbol": token_info.get('symbol', 'UNK'),
                                    "direction": direction
                                }
                            ),
                            'whale_activity'
                        )
    
    async def _monitor_whale_activity(self):
        """Monitora atividade de baleias em tokens observados"""
        while True:
            try:
                for token_address in list(self.watched_tokens):
                    whale_activity = await self.dex_analyzer.detect_whale_activity(token_address)
                    for activity in whale_activity:
                        token_info = await self.token_analyzer.get_token_info(token_address)
                        
                        AlertSystem.send_alert(
                            AlertSystem.format_alert(
                                "WHALE_ACTIVITY",
                                self.chain,
                                {
                                    "address": activity['sender'],
                                    "token": token_address,
                                    "amount": Utils.wei_to_eth(int(activity['amount'])),
                                    "symbol": token_info.get('symbol', 'UNK'),
                                    "direction": activity['direction']
                                }
                            ),
                            'whale_activity'
                        )
                
                await asyncio.sleep(60)  # Verificar a cada minuto
            except Exception as e:
                trace.log_event(
                    "WHALE_MONITOR",
                    "ERROR",
                    {
                        "error": str(e)
                    }
                )
                await asyncio.sleep(30)
    
    async def start_mempool_monitoring(self):
        """Inicia o monitoramento do mempool"""
        await self.connector.listen_for_pending_transactions(self._handle_pending_transaction)
    
    def _handle_pending_transaction(self, tx: Dict):
        """Manipula uma nova transação pendente"""
        asyncio.create_task(self.pending_tx_queue.put(tx))
    
    def watch_token(self, token_address: str):
        """Adiciona um token à lista de observação"""
        self.watched_tokens.add(Web3.to_checksum_address(token_address))
    
    def watch_wallet(self, wallet_address: str):
        """Adiciona uma carteira à lista de observação"""
        self.watched_wallets.add(Web3.to_checksum_address(wallet_address))
    
    def _load_known_scammers(self) -> Set[str]:
        """Carrega endereços de scammers conhecidos a partir de arquivo local."""
        try:
            data_file = Path(__file__).resolve().parents[1] / "data" / "multichain.json"
            if data_file.exists():
                with open(data_file, "r") as f:
                    data = json.load(f)
                return set(data.get(self.chain, []))
        except Exception as e:
            trace.log_event(
                "SCAMMER_LIST_LOAD",
                "ERROR",
                {
                    "error": str(e)
                }
            )
        return set()
    
    @trace.trace_operation
    async def track_token_contract(self, token_address: str) -> Dict:
        """Rastreia um contrato de token e analisa seus dados"""
        token_address = Web3.to_checksum_address(token_address)
        self.watch_token(token_address)
        
        token_contract = await self.connector.get_contract(token_address)
        self.token_cache[token_address] = token_contract
        
        token_info = await self.token_analyzer.get_token_info(token_address)
        events_data = await self.token_analyzer.analyze_token_events(token_address)
        await self._process_holders(token_address, events_data['transfers'])
        
        analysis_results = await self._analyze_holders_behavior(token_address, events_data)
        liquidity_data = await self.token_analyzer.analyze_liquidity(token_address)
        wash_trading = await self.token_analyzer.detect_wash_trading(token_address)
        pump_and_dump = self.ai_engine.detect_pump_and_dump(events_data)
        
        token_data = {
            'token_info': token_info,
            'events_data': {
                'transfer_count': len(events_data['transfers']),
                'swap_count': len(events_data['swaps']),
                'add_liquidity_count': len(events_data['add_liquidity']),
                'remove_liquidity_count': len(events_data['remove_liquidity'])
            },
            'analysis_results': analysis_results,
            'liquidity_data': liquidity_data,
            'wash_trading': wash_trading,
            'pump_and_dump': pump_and_dump,
            'metadata': {
                'analysis_timestamp': int(time.time()),
                'chain': self.chain,
                'module_version': ModuleIdentity.VERSION
            }
        }
        
        self._save_token_data(token_address, token_data)
        return token_data
    
    async def _process_holders(self, token_address: str, transfer_events: List[Dict]) -> None:
        """Processa eventos de transferência para atualizar holders"""
        for event in transfer_events:
            from_address = event['args']['from']
            to_address = event['args']['to']
            value = event['args']['value']
            
            if from_address == '0x0000000000000000000000000000000000000000':
                self.holders[token_address][to_address] = self.holders[token_address].get(to_address, 0) + value
            elif to_address == '0x0000000000000000000000000000000000000000':
                self.holders[token_address][from_address] = self.holders[token_address].get(from_address, 0) - value
            else:
                self.holders[token_address][from_address] = self.holders[token_address].get(from_address, 0) - value
                self.holders[token_address][to_address] = self.holders[token_address].get(to_address, 0) + value
        
        # Remover holders com saldo zero
        self.holders[token_address] = {k: v for k, v in self.holders[token_address].items() if v > 0}
    
    async def _analyze_holders_behavior(self, token_address: str, events_data: Dict) -> Dict:
        """Analisa o comportamento dos holders de um token"""
        transfer_events = sorted(events_data['transfers'], key=lambda x: x['blockNumber'])
        early_receivers = {}
        
        # Identificar primeiros receivers
        for event in transfer_events[:self.config.thresholds.early_adopter_window]:
            to_address = event['args']['to']
            if to_address not in early_receivers:
                early_receivers[to_address] = {
                    'block_number': event['blockNumber'],
                    'timestamp': (await self.connector.get_block(event['blockNumber']))['timestamp'],
                    'amount': str(event['args']['value'])
                }
        
        # Identificar grandes swappers
        large_swappers = set()
        for event in events_data['swaps']:
            if (int(event['args']['amount0In']) >= self.config.thresholds.min_swap_amount_eth * 10**18 or 
                int(event['args']['amount1In']) >= self.config.thresholds.min_swap_amount_eth * 10**18):
                large_swappers.add(event['args']['sender'])
        
        # Identificar provedores de liquidez
        liquidity_providers = set()
        for event in events_data['add_liquidity']:
            liquidity_providers.add(event['args']['sender'])
        
        # Identificar removedores de liquidez
        liquidity_removers = set()
        for event in events_data['remove_liquidity']:
            liquidity_removers.add(event['args']['sender'])
        
        # Top holders
        top_holders = sorted(self.holders[token_address].items(), key=lambda x: x[1], reverse=True)[:100]
        
        return {
            'early_adopters': early_receivers,
            'large_swappers': list(large_swappers),
            'liquidity_providers': list(liquidity_providers),
            'liquidity_removers': list(liquidity_removers),
            'top_holders': [{'address': addr, 'balance': str(bal)} for addr, bal in top_holders],
            'holder_distribution': await self._calculate_holder_distribution(token_address)
        }
    
    async def _calculate_holder_distribution(self, token_address: str) -> Dict:
        """Calcula a distribuição de holders"""
        balances = list(self.holders[token_address].values())
        if not balances:
            return {}
        
        total_supply = sum(balances)
        if total_supply == 0:
            return {}
        
        sorted_balances = sorted(balances, reverse=True)
        top10 = sum(sorted_balances[:10]) / total_supply
        top50 = sum(sorted_balances[:50]) / total_supply
        top100 = sum(sorted_balances[:100]) / total_supply
        
        return {
            'total_holders': len(balances),
            'top10_percent': float(top10),
            'top50_percent': float(top50),
            'top100_percent': float(top100),
            'gini_coefficient': Utils.calculate_gini_coefficient(balances)
        }
    
    def _save_token_data(self, token_address: str, data: Dict) -> None:
        """Salva dados do token em arquivo"""
        filename = os.path.join(self.config.data_dir, f"{self.chain}_{token_address}_{int(time.time())}.json")
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    
    @trace.trace_operation
    async def detect_smart_money(self, token_address: str, min_profit_trades: int = 3) -> List[str]:
        """Detecta carteiras de 'smart money'"""
        trades = await self._get_token_trades(token_address)
        if not trades:
            return []
        
        address_trades = defaultdict(list)
        for trade in trades:
            address_trades[trade['from']].append(trade)
        
        smart_money = []
        for address, trades in address_trades.items():
            if len(trades) < 3:
                continue
            
            profitable_trades = 0
            for i in range(1, len(trades)):
                if trades[i]['price'] > trades[i-1]['price'] * 1.1:
                    profitable_trades += 1
            
            if profitable_trades >= min_profit_trades:
                smart_money.append(address)
                self.smart_money_wallets.add(address)
                self.watch_wallet(address)
        
        return smart_money
    
    async def _get_token_trades(self, token_address: str) -> List[Dict]:
        """Obtém trades de um token"""
        pools = await self.token_analyzer.get_liquidity_pools(token_address)
        if not pools:
            return []

        pool_info = {}
        for pool_address in pools:
            try:
                contract = await self.connector.get_contract(pool_address)
                token0 = await contract.functions.token0().call()
                token1 = await contract.functions.token1().call()
                pool_info[pool_address] = (contract, token0, token1)
            except Exception as e:
                trace.log_event(
                    "TRADE_FETCH",
                    "ERROR",
                    {"pool": pool_address, "error": str(e)}
                )
                continue

        trades = []
        start_block = await self.block_scanner.get_saved_block()
        swap_topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

        async for block in self.block_scanner.scan(start_block):
            tx_hashes = [h.hex() if isinstance(h, bytes) else h for h in block['transactions']]
            receipts = await asyncio.gather(
                *[self.connector.get_transaction_receipt(h) for h in tx_hashes],
                return_exceptions=True
            )
            for receipt in receipts:
                if isinstance(receipt, Exception):
                    continue
                for log in receipt['logs']:
                    if log['address'] in pool_info and len(log['topics']) > 0 and log['topics'][0].hex() == swap_topic:
                        contract, token0, token1 = pool_info[log['address']]
                        try:
                            event = contract.events.Swap().process_log(log)
                            if token_address.lower() == token0.lower():
                                amount = int(event['args']['amount0In']) - int(event['args']['amount0Out'])
                                price = (
                                    abs(int(event['args']['amount1Out']) / int(event['args']['amount0In']))
                                    if int(event['args']['amount0In']) > 0 else None
                                )
                            else:
                                amount = int(event['args']['amount1In']) - int(event['args']['amount1Out'])
                                price = (
                                    abs(int(event['args']['amount0Out']) / int(event['args']['amount1In']))
                                    if int(event['args']['amount1In']) > 0 else None
                                )

                            if price is not None:
                                trades.append({
                                    'from': receipt['from'],
                                    'to': receipt['to'],
                                    'amount': str(abs(amount)),
                                    'price': float(price),
                                    'timestamp': block['timestamp'],
                                    'tx_hash': receipt['transactionHash'].hex()
                                })
                        except Exception as e:
                            trace.log_event(
                                "TRADE_FETCH",
                                "ERROR",
                                {"pool": log['address'], "error": str(e)}
                            )
                            continue

        return trades
    
    @trace.trace_operation
    async def track_deployers(self, token_address: str) -> Dict:
        """Rastreia e analisa os implementadores de um token"""
        deployer_address = await self._get_contract_deployer(token_address)
        self.deployers[token_address] = deployer_address
        self.watch_wallet(deployer_address)
        
        deployer_activity = await self._monitor_deployer_activity(deployer_address, token_address)
        risk_indicators = await self._assess_deployer_risk(deployer_address, token_address)
        
        if risk_indicators:
            AlertSystem.send_alert(
                AlertSystem.format_alert(
                    "SUSPICIOUS_DEPLOYER",
                    self.chain,
                    {
                        "token_address": token_address,
                        "deployer_address": deployer_address,
                        "risk_indicators": risk_indicators
                    }
                ),
                'high_risk'
            )
        
        return {
            'deployer_address': deployer_address,
            'activities': deployer_activity,
            'risk_indicators': risk_indicators
        }
    
    async def _get_contract_deployer(self, token_address: str) -> str:
        """Obtém o endereço do implementador do contrato"""
        try:
            # Primeiro verifica se já temos essa informação em cache
            if token_address in self.deployers:
                return self.deployers[token_address]

            transfer_sig = Web3.keccak(text="Transfer(address,address,uint256)").hex()
            logs = await self.connector.async_w3.eth.get_logs({
                'fromBlock': 0,
                'toBlock': 'latest',
                'address': Web3.to_checksum_address(token_address),
                'topics': [transfer_sig]
            })

            if logs:
                tx_hash = logs[0]['transactionHash'].hex()
                tx = await self.connector.get_transaction(tx_hash)
                return tx['from']

            # Fallback: varre os últimos blocos em busca da transação de criação
            latest = await self.connector.async_w3.eth.block_number
            start_block = max(0, latest - 10000)

            for block_num in range(start_block, latest + 1):
                block = await self.connector.get_block(block_num)
                for th in block['transactions']:
                    tx = await self.connector.get_transaction(th.hex() if isinstance(th, bytes) else th)
                    if tx['to'] is None:
                        receipt = await self.connector.get_transaction_receipt(th.hex() if isinstance(th, bytes) else th)
                        if receipt.contractAddress and receipt.contractAddress.lower() == token_address.lower():
                            return tx['from']

            return '0xUnknownDeployer'
        except Exception as e:
            trace.log_event(
                "DEPLOYER_FETCH",
                "ERROR",
                {
                    "token": token_address,
                    "error": str(e)
                }
            )
            return '0xUnknownDeployer'
    
    async def _monitor_deployer_activity(self, deployer_address: str, token_address: str) -> Dict:
        """Monitora a atividade do implementador"""
        token_contract = self.token_cache.get(token_address)
        if not token_contract:
            token_contract = await self.connector.get_contract(token_address)
            self.token_cache[token_address] = token_contract
        
        initial_balance = await token_contract.functions.balanceOf(deployer_address).call()
        has_initial_supply = initial_balance > 0
        
        liquidity_removed = False
        tokens_dumped = False
        
        transfer_events = await token_contract.events.Transfer.get_logs(
            fromBlock='earliest',
            argument_filters={'from': deployer_address}
        )
        
        if transfer_events:
            tokens_dumped = True
        
        pools = await self.token_analyzer.get_liquidity_pools(token_address)
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                burn_events = await pool_contract.events.Burn.get_logs(
                    fromBlock='earliest',
                    argument_filters={'sender': deployer_address}
                )
                
                if burn_events:
                    liquidity_removed = True
                    break
            except Exception as e:
                trace.log_event(
                    "DEPLOYER_ACTIVITY",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
                )
                continue
        
        return {
            'has_initial_supply': has_initial_supply,
            'initial_balance': str(initial_balance),
            'liquidity_removed': liquidity_removed,
            'tokens_dumped': tokens_dumped,
            'other_deploys': await self._check_other_deploys(deployer_address),
            'is_known_scammer': deployer_address in self.known_scammers
        }
    
    async def _check_other_deploys(self, deployer_address: str) -> List[Dict]:
        """Varre blocos recentes em busca de outros contratos do mesmo deployer."""
        try:
            contracts = []
            start_block = await self.block_scanner.get_saved_block()

            async for block in self.block_scanner.scan(start_block):
                tx_hashes = [h.hex() if isinstance(h, bytes) else h for h in block['transactions']]
                txs = await asyncio.gather(
                    *[self.connector.get_transaction(h) for h in tx_hashes],
                    return_exceptions=True
                )

                receipt_tasks = []
                valid_hashes = []
                for th, tx in zip(tx_hashes, txs):
                    if isinstance(tx, Exception):
                        continue
                    if tx['from'].lower() == deployer_address.lower() and tx['to'] is None:
                        receipt_tasks.append(self.connector.get_transaction_receipt(th))
                        valid_hashes.append(th)

                receipts = await asyncio.gather(*receipt_tasks, return_exceptions=True)
                for th, receipt in zip(valid_hashes, receipts):
                    if isinstance(receipt, Exception):
                        continue
                    if getattr(receipt, 'contractAddress', None):
                        contracts.append({
                            'contract_address': receipt.contractAddress,
                            'timestamp': int(block['timestamp']),
                            'tx_hash': th,
                        })
                if len(contracts) >= 10:
                    break

            return contracts
        except Exception as e:
            trace.log_event(
                "DEPLOYER_HISTORY",
                "ERROR",
                {
                    "deployer": deployer_address,
                    "error": str(e)
                }
            )
            return []
    
    async def _assess_deployer_risk(self, deployer_address: str, token_address: str) -> List[str]:
        """Avalia os riscos associados ao implementador"""
        risk_indicators = []
        token_contract = self.token_cache.get(token_address)
        
        if token_contract:
            deployer_balance = await token_contract.functions.balanceOf(deployer_address).call()
            total_supply = await token_contract.functions.totalSupply().call()
            
            if total_supply > 0 and (deployer_balance / total_supply) > self.config.thresholds.risk_threshold:
                risk_indicators.append('high_supply_concentration')
        
        activities = await self._monitor_deployer_activity(deployer_address, token_address)
        if activities['liquidity_removed']:
            risk_indicators.append('liquidity_removal')
        if activities['tokens_dumped']:
            risk_indicators.append('token_dumping')
        if activities['is_known_scammer']:
            risk_indicators.append('known_scammer')
        if activities['other_deploys']:
            risk_indicators.append('multiple_deploys')
        
        return risk_indicators
    
    @trace.trace_operation
    async def get_top_holders(self, token_address: str, n: int = 10) -> List[Dict]:
        """Obtém os maiores holders de um token"""
        if token_address not in self.holders:
            await self.track_token_contract(token_address)
        
        holders = self.holders.get(token_address, {})
        sorted_holders = sorted(holders.items(), key=lambda x: x[1], reverse=True)
        return [{'address': addr, 'balance': str(bal)} for addr, bal in sorted_holders[:n]]
    
    @trace.trace_operation
    async def generate_watchlist(self, token_address: str) -> Dict:
        """Gera uma lista de observação para um token"""
        token_data = await self.track_token_contract(token_address)
        smart_money = await self.detect_smart_money(token_address)
        deployer_info = await self.track_deployers(token_address)
        
        watchlist = {
            'token_info': token_data['token_info'],
            'early_adopters': token_data['analysis_results']['early_adopters'],
            'large_swappers': token_data['analysis_results']['large_swappers'],
            'liquidity_providers': token_data['analysis_results']['liquidity_providers'],
            'liquidity_removers': token_data['analysis_results']['liquidity_removers'],
            'smart_money': smart_money,
            'deployer': deployer_info,
            'holder_distribution': token_data['analysis_results']['holder_distribution'],
            'liquidity_data': token_data['liquidity_data'],
            'wash_trading': token_data['wash_trading'],
            'pump_and_dump': token_data['pump_and_dump'],
            'generated_at': int(time.time()),
            'chain': self.chain,
            'module_version': ModuleIdentity.VERSION
        }
        
        filename = os.path.join(self.config.data_dir, f"watchlist_{self.chain}_{token_address}_{int(time.time())}.json")
        with open(filename, 'w') as f:
            json.dump(watchlist, f, indent=2)
        
        return watchlist
    
    @trace.trace_operation
    async def generate_risk_report(self, token_address: str) -> Dict:
        """Gera um relatório de risco para um token"""
        watchlist = await self.generate_watchlist(token_address)
        
        risk_score = 0
        risk_factors = []
        
        # Holder concentration risk
        top10_percent = watchlist['holder_distribution']['top10_percent']
        if top10_percent > 0.5:
            risk_score += 3
            risk_factors.append(f"High holder concentration (top 10% hold {top10_percent*100:.1f}% of supply)")
        elif top10_percent > 0.3:
            risk_score += 1
            risk_factors.append(f"Moderate holder concentration (top 10% hold {top10_percent*100:.1f}% of supply)")
        
        # Deployer risk
        deployer_risks = watchlist['deployer']['risk_indicators']
        if deployer_risks:
            risk_score += len(deployer_risks) * 2
            risk_factors.extend([f"Deployer risk: {risk}" for risk in deployer_risks])
        
        # Liquidity risk
        liquidity_usd = watchlist['liquidity_data'].get('total_liquidity_usd', 0)
        if liquidity_usd < 10000:
            risk_score += 2
            risk_factors.append(f"Low liquidity (${liquidity_usd:,.2f})")
        elif liquidity_usd < 50000:
            risk_score += 1
            risk_factors.append(f"Moderate liquidity (${liquidity_usd:,.2f})")
        
        # Wash trading
        if watchlist['wash_trading']:
            risk_score += 2
            risk_factors.append(f"Potential wash trading detected in {len(watchlist['wash_trading'])} pools")
        
        # Pump and dump
        if watchlist['pump_and_dump']['score'] > 0.7:
            risk_score += 3
            risk_factors.append(f"Pump & dump detected (score: {watchlist['pump_and_dump']['score']:.2f})")
        elif watchlist['pump_and_dump']['score'] > 0.5:
            risk_score += 1
            risk_factors.append(f"Suspicious price movement (score: {watchlist['pump_and_dump']['score']:.2f})")
        
        # Liquidity removal
        if watchlist['liquidity_removers']:
            risk_score += 2
            risk_factors.append(f"Liquidity removed by {len(watchlist['liquidity_removers'])} addresses")
        
        # Normalizar pontuação de risco para 0-10
        risk_score = min(10, risk_score)
        
        report = {
            'token_address': token_address,
            'risk_score': risk_score,
            'risk_level': 'high' if risk_score >= 7 else 'medium' if risk_score >= 4 else 'low',
            'risk_factors': risk_factors,
            'timestamp': int(time.time()),
            'chain': self.chain,
            'module_version': ModuleIdentity.VERSION
        }
        
        if risk_score >= self.config.thresholds.high_risk_score:
            AlertSystem.send_alert(
                AlertSystem.format_alert(
                    "HIGH_RISK",
                    self.chain,
                    report
                ),
                'high_risk'
            )
        
        return report
    
    @trace.trace_operation
    async def classify_wallet(self, wallet_address: str) -> Dict:
        """Classifica uma carteira com base em seu comportamento"""
        wallet_history = await self._get_wallet_history(wallet_address)
        wallet_type = self.ai_engine.classify_wallet(wallet_history)
        
        return {
            'wallet_address': wallet_address,
            'wallet_type': wallet_type,
            'activities': wallet_history,
            'timestamp': int(time.time()),
            'chain': self.chain,
            'module_version': ModuleIdentity.VERSION
        }
    
    async def _get_wallet_history(self, wallet_address: str) -> Dict:
        """Obtém o histórico de uma carteira"""
        history = {
            'transactions': [],
            'swaps': [],
            'holdings': {},
            'is_deployer': False,
            'liquidity_removed': False,
            'tokens_dumped': False
        }
        
        # Obter transações regulares varrendo blocos via BlockScanner
        start_block = await self.block_scanner.get_saved_block()

        async for block in self.block_scanner.scan(start_block):
            tx_hashes = [h.hex() if isinstance(h, bytes) else h for h in block['transactions']]
            txs = await asyncio.gather(
                *[self.connector.get_transaction(h) for h in tx_hashes],
                return_exceptions=True
            )
            for th, tx in zip(tx_hashes, txs):
                if isinstance(tx, Exception):
                    continue
                if tx['from'].lower() == wallet_address.lower() or (
                    tx.get('to') and tx['to'].lower() == wallet_address.lower()
                ):
                    history['transactions'].append({
                        'hash': th,
                        'from': tx['from'],
                        'to': tx.get('to'),
                        'value': str(tx['value']),
                        'blockNumber': block['number'],
                    })
        
        # Verificar se é implementador
        for token, deployer in self.deployers.items():
            if deployer == wallet_address:
                history['is_deployer'] = True
                break
        
        # Obter holdings e swaps
        for token_address in self.holders:
            if wallet_address in self.holders[token_address]:
                history['holdings'][token_address] = {
                    'balance': str(self.holders[token_address][wallet_address])
                }
        
        return history

# ==============================================
