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
from .blockchain import BlockchainConnector
from .utils import Utils, trace

# ==============================================
# Módulo: Análise de Tokens
# ==============================================

class TokenAnalyzer:
    """Analisador avançado de tokens"""
    
    def __init__(self, connector: BlockchainConnector):
        self.connector = connector
        self.config = ModuleConfig()
        self.dex_routers = {
            'ethereum': {
                'uniswap': '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
                'sushiswap': '0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F'
            },
            'bsc': {
                'pancakeswap': '0x10ED43C718714eb63d5aA57B78B54704E256024E'
            },
            'polygon': {
                'quickswap': '0xa5E0829CaCEd8fFDD4De3c43696c57F7D7A678ff'
            }
        }.get(connector.chain, {})
    
    @trace.trace_operation
    async def get_token_info(self, token_address: str) -> Dict:
        """Obtém informações básicas de um token"""
        token_address = Web3.to_checksum_address(token_address)
        token_contract = await self.connector.get_contract(token_address)
        
        try:
            info = {
                'name': await token_contract.functions.name().call(),
                'symbol': await token_contract.functions.symbol().call(),
                'decimals': await token_contract.functions.decimals().call(),
                'total_supply': str(await token_contract.functions.totalSupply().call()),
                'is_verified': True
            }
        except Exception as e:
            info = {
                'name': 'Unknown',
                'symbol': 'UNK',
                'decimals': 18,
                'total_supply': '0',
                'is_verified': False
            }
        
        info['token_standard'] = await self._detect_token_standard(token_contract)
        return info
    
    async def _detect_token_standard(self, token_contract: Any) -> str:
        """Detecta o padrão do token (ERC20, ERC721, ERC1155)"""
        try:
            if await token_contract.functions.supportsInterface('0x80ac58cd').call():  # ERC721
                return 'ERC721'
            elif await token_contract.functions.supportsInterface('0xd9b67a26').call():  # ERC1155
                return 'ERC1155'
            else:
                return 'ERC20'
        except:
            return 'ERC20'
    
    @trace.trace_operation
    async def get_liquidity_pools(self, token_address: str) -> List[str]:
        """Identifica pools de liquidez para um token"""
        cache_key = f"{self.connector.chain}_{token_address}_pools"
        cached_pools = await self.config.get_cache(cache_key)
        if cached_pools:
            return cached_pools
        
        pools = set()
        
        # Verificar transações de criação de par
        token_contract = await self.connector.get_contract(token_address)
        creation_events = await token_contract.events.PairCreated.get_logs(fromBlock='earliest')
        
        for event in creation_events:
            if event['args']['token0'].lower() == token_address.lower() or event['args']['token1'].lower() == token_address.lower():
                pools.add(event['args']['pair'])
        
        # Verificar transações com roteadores conhecidos
        for router_address in self.dex_routers.values():
            try:
                router_contract = await self.connector.get_contract(router_address)
                swap_events = await router_contract.events.Swap.get_logs(
                    fromBlock='earliest',
                    argument_filters={'tokenIn': token_address}
                )
                
                for event in swap_events:
                    pools.add(event['address'])
            except Exception as e:
                trace.log_event(
                    "POOL_DETECTION",
                    "WARNING",
                    {
                        "router": router_address,
                        "error": str(e)
                    }
                )
                continue
        
        pools = list(pools)
        await self.config.set_cache(cache_key, pools)
        return pools
    
    @trace.trace_operation
    async def analyze_liquidity(self, token_address: str) -> Dict:
        """Analisa a liquidez de um token"""
        pools = await self.get_liquidity_pools(token_address)
        if not pools:
            return {
                'total_liquidity': '0',
                'total_liquidity_usd': 0.0,
                'pools': []
            }
        
        total_liquidity = 0
        total_liquidity_usd = 0.0
        pool_details = []
        
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                
                # Obter reservas
                reserves = await pool_contract.functions.getReserves().call()
                token0 = await pool_contract.functions.token0().call()
                token1 = await pool_contract.functions.token1().call()
                
                # Determinar qual token é o nosso
                if token0.lower() == token_address.lower():
                    token_liquidity = reserves[0]
                    other_token = token1
                else:
                    token_liquidity = reserves[1]
                    other_token = token0
                
                # Obter preço aproximado (simplificado)
                price = await self._estimate_token_price(pool_address, token_address)
                liquidity_usd = float(token_liquidity) * price if price else 0.0
                
                pool_details.append({
                    'pool_address': pool_address,
                    'token_liquidity': str(token_liquidity),
                    'other_token': other_token,
                    'liquidity_usd': liquidity_usd,
                    'price': price
                })
                
                total_liquidity += int(token_liquidity)
                total_liquidity_usd += liquidity_usd
            except Exception as e:
                trace.log_event(
                    "LIQUIDITY_ANALYSIS",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
                )
                continue
        
        return {
            'total_liquidity': str(total_liquidity),
            'total_liquidity_usd': total_liquidity_usd,
            'pools': pool_details
        }
    
    async def _estimate_token_price(self, pool_address: str, token_address: str) -> Optional[float]:
        """Estima o preço de um token baseado na pool"""
        try:
            pool_contract = await self.connector.get_contract(pool_address)
            
            token0 = await pool_contract.functions.token0().call()
            token1 = await pool_contract.functions.token1().call()
            reserves = await pool_contract.functions.getReserves().call()
            
            if token0.lower() == token_address.lower():
                return reserves[1] / reserves[0] if reserves[0] > 0 else None
            else:
                return reserves[0] / reserves[1] if reserves[1] > 0 else None
        except Exception:
            return None
    
    @trace.trace_operation
    async def detect_wash_trading(self, token_address: str) -> List[Dict]:
        """Detecta possíveis wash trades para um token"""
        pools = await self.get_liquidity_pools(token_address)
        if not pools:
            return []
        
        wash_trades = []
        
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                swap_events = await pool_contract.events.Swap.get_logs(fromBlock='earliest')
                
                # Agrupar swaps por endereço
                address_swaps = defaultdict(list)
                for event in swap_events:
                    address_swaps[event['args']['sender']].append({
                        'amount0In': event['args']['amount0In'],
                        'amount1In': event['args']['amount1In'],
                        'amount0Out': event['args']['amount0Out'],
                        'amount1Out': event['args']['amount1Out'],
                        'timestamp': (await self.connector.get_block(event['blockNumber']))['timestamp'],
                        'tx_hash': event['transactionHash'].hex()
                    })
                
                # Analisar padrões de wash trading
                for address, swaps in address_swaps.items():
                    if len(swaps) < 3:
                        continue
                    
                    # Verificar se há compras e vendas rápidas
                    buy_sell_pairs = []
                    for i in range(len(swaps) - 1):
                        time_diff = swaps[i+1]['timestamp'] - swaps[i]['timestamp']
                        if time_diff < 3600:  # 1 hora
                            buy_sell_pairs.append((swaps[i], swaps[i+1]))
                    
                    if not buy_sell_pairs:
                        continue
                    
                    # Verificar se os valores são similares
                    similar_amounts = 0
                    for buy, sell in buy_sell_pairs:
                        buy_amount = buy['amount0In'] if buy['amount0In'] > 0 else buy['amount1In']
                        sell_amount = sell['amount0Out'] if sell['amount0Out'] > 0 else sell['amount1Out']
                        
                        if abs(buy_amount - sell_amount) < (buy_amount * 0.1):  # 10% de diferença
                            similar_amounts += 1
                    
                    if similar_amounts / len(buy_sell_pairs) > 0.7:  # 70% de similaridade
                        wash_trades.append({
                            'pool_address': pool_address,
                            'address': address,
                            'tx_count': len(swaps),
                            'buy_sell_pairs': len(buy_sell_pairs),
                            'similar_amounts': similar_amounts,
                            'first_tx': min(s['timestamp'] for s in swaps),
                            'last_tx': max(s['timestamp'] for s in swaps)
                        })
            except Exception as e:
                trace.log_event(
                    "WASH_TRADE_DETECTION",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
                )
                continue
        
        return wash_trades
    
    @trace.trace_operation
    async def analyze_token_events(self, token_address: str) -> Dict:
        """Analisa todos os eventos relevantes de um token"""
        token_contract = await self.connector.get_contract(token_address)
        
        # Obter eventos de transferência
        transfer_events = await token_contract.events.Transfer.get_logs(fromBlock='earliest')
        
        # Obter eventos de liquidez
        pools = await self.get_liquidity_pools(token_address)
        swap_events = []
        add_liquidity_events = []
        remove_liquidity_events = []
        
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                
                # Swap events
                swaps = await pool_contract.events.Swap.get_logs(fromBlock='earliest')
                swap_events.extend(swaps)
                
                # Mint events (add liquidity)
                mints = await pool_contract.events.Mint.get_logs(fromBlock='earliest')
                add_liquidity_events.extend(mints)
                
                # Burn events (remove liquidity)
                burns = await pool_contract.events.Burn.get_logs(fromBlock='earliest')
                remove_liquidity_events.extend(burns)
            except Exception as e:
                trace.log_event(
                    "TOKEN_EVENTS",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
                )
                continue
        
        return {
            'transfers': transfer_events,
            'swaps': swap_events,
            'add_liquidity': add_liquidity_events,
            'remove_liquidity': remove_liquidity_events
        }

# ==============================================
# Módulo: Análise DEX
# ==============================================

class DexAnalyzer:
    """Analisador de trocas em DEXs"""
    
    def __init__(self, connector: BlockchainConnector):
        self.connector = connector
        self.config = ModuleConfig()
        self.token_analyzer = TokenAnalyzer(connector)
    
    @trace.trace_operation
    async def analyze_swap(self, tx_hash: str) -> Dict:
        """Analisa uma transação de swap"""
        receipt = await self.connector.get_transaction_receipt(tx_hash)
        tx = await self.connector.get_transaction(tx_hash)
        
        swap_info = {
            'tx_hash': tx_hash,
            'from': tx['from'],
            'to': tx['to'],
            'value': str(tx['value']),
            'gas_price': str(tx['gasPrice']),
            'gas_used': str(receipt['gasUsed']),
            'timestamp': datetime.now(pytz.utc).isoformat(),
            'chain': self.connector.chain,
            'events': []
        }
        
        for log in receipt['logs']:
            try:
                if len(log['topics']) == 0:
                    continue
                
                # Handle Swap events
                if log['topics'][0].hex() == '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822':
                    contract = await self.connector.get_contract(log['address'])
                    event = contract.events.Swap().process_log(log)
                    
                    swap_info['events'].append({
                        'type': 'swap',
                        'contract': log['address'],
                        'sender': event['args']['sender'],
                        'to': event['args']['to'],
                        'amount0_in': str(event['args']['amount0In']),
                        'amount1_in': str(event['args']['amount1In']),
                        'amount0_out': str(event['args']['amount0Out']),
                        'amount1_out': str(event['args']['amount1Out'])
                    })
                
                # Handle Mint events (add liquidity)
                elif log['topics'][0].hex() == '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f':
                    contract = await self.connector.get_contract(log['address'])
                    event = contract.events.Mint().process_log(log)
                    
                    swap_info['events'].append({
                        'type': 'mint',
                        'contract': log['address'],
                        'sender': event['args']['sender'],
                        'amount0': str(event['args']['amount0']),
                        'amount1': str(event['args']['amount1'])
                    })
                
                # Handle Burn events (remove liquidity)
                elif log['topics'][0].hex() == '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496':
                    contract = await self.connector.get_contract(log['address'])
                    event = contract.events.Burn().process_log(log)
                    
                    swap_info['events'].append({
                        'type': 'burn',
                        'contract': log['address'],
                        'sender': event['args']['sender'],
                        'amount0': str(event['args']['amount0']),
                        'amount1': str(event['args']['amount1']),
                        'to': event['args']['to']
                    })
                
                # Handle Sync events (reserve updates)
                elif log['topics'][0].hex() == '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1':
                    contract = await self.connector.get_contract(log['address'])
                    event = contract.events.Sync().process_log(log)
                    
                    swap_info['events'].append({
                        'type': 'sync',
                        'contract': log['address'],
                        'reserve0': str(event['args']['reserve0']),
                        'reserve1': str(event['args']['reserve1'])
                    })
            
            except Exception as e:
                trace.log_event(
                    "LOG_PROCESSING",
                    "ERROR",
                    {
                        "tx_hash": tx_hash,
                        "log_index": log['logIndex'],
                        "error": str(e)
                    }
                )
                continue
        
        return swap_info
    
    def get_swap_direction(self, swap_event: Dict) -> str:
        """Determina a direção de um swap (compra/venda)"""
        if int(swap_event['amount0_in']) > 0 and int(swap_event['amount1_out']) > 0:
            return 'buy'
        elif int(swap_event['amount1_in']) > 0 and int(swap_event['amount0_out']) > 0:
            return 'sell'
        return 'unknown'
    
    @trace.trace_operation
    async def detect_whale_activity(self, token_address: str, time_window: int = 3600) -> List[Dict]:
        """Detecta atividade de baleias em um token"""
        pools = await self.token_analyzer.get_liquidity_pools(token_address)
        if not pools:
            return []
        
        whale_swaps = []
        threshold = self.config.thresholds.whale_threshold_eth * 10**18
        
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                
                # Obter eventos de swap recentes
                latest_block = await self.connector.get_block('latest')
                from_block = latest_block['number'] - int(time_window / 15)  # ~15s por bloco
                
                swap_events = await pool_contract.events.Swap.get_logs(
                    fromBlock=max(0, from_block),
                    toBlock='latest'
                )
                
                for event in swap_events:
                    # Verificar se o swap é grande o suficiente
                    amount_in = max(event['args']['amount0In'], event['args']['amount1In'])
                    amount_out = max(event['args']['amount0Out'], event['args']['amount1Out'])
                    
                    if amount_in >= threshold or amount_out >= threshold:
                        direction = 'buy' if event['args']['amount0In'] > 0 else 'sell'
                        
                        whale_swaps.append({
                            'pool_address': pool_address,
                            'tx_hash': event['transactionHash'].hex(),
                            'sender': event['args']['sender'],
                            'amount': str(max(amount_in, amount_out)),
                            'direction': direction,
                            'timestamp': (await self.connector.get_block(event['blockNumber']))['timestamp']
                        })
            except Exception as e:
                trace.log_event(
                    "WHALE_DETECTION",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
                )
                continue
        
        return whale_swaps

# ==============================================
# Módulo: Machine Learning
# ==============================================

class AIBehaviorEngine:
    """Motor de análise comportamental baseado em ML"""
    
    def __init__(self):
        self.config = ModuleConfig().ml_config
        self.model = None
        self.scaler = StandardScaler()
        self.dbscan = DBSCAN(eps=0.5, min_samples=5)
        self.lof = LocalOutlierFactor(n_neighbors=20, contamination=0.1)
        self.last_trained = 0
        self.load_models()
    
    @trace.trace_operation
    def load_models(self):
        """Carrega os modelos de ML ou inicializa novos"""
        try:
            if os.path.exists(self.config.model_path):
                self.model = joblib.load(self.config.model_path)
            else:
                self.model = IsolationForest(
                    contamination=self.config.contamination,
                    random_state=self.config.random_state
                )
            
            if os.path.exists(self.config.scaler_path):
                self.scaler = joblib.load(self.config.scaler_path)
        except Exception as e:
            trace.log_event(
                "ML_MODEL_LOAD",
                "ERROR",
                {
                    "error": str(e)
                }
            )
            self.model = IsolationForest(
                contamination=self.config.contamination,
                random_state=self.config.random_state
            )
    
    @trace.trace_operation
    def save_models(self):
        """Salva os modelos treinados"""
        os.makedirs(os.path.dirname(self.config.model_path), exist_ok=True)
        
        try:
            joblib.dump(self.model, self.config.model_path)
            joblib.dump(self.scaler, self.config.scaler_path)
            self.last_trained = time.time()
        except Exception as e:
            trace.log_event(
                "ML_MODEL_SAVE",
                "ERROR",
                {
                    "error": str(e)
                }
            )
    
    @trace.trace_operation
    def train_model(self, X: np.ndarray):
        """Treina o modelo com novos dados"""
        try:
            X_scaled = self.scaler.fit_transform(X)
            self.model.fit(X_scaled)
            
            # Treinar modelos auxiliares
            self.dbscan.fit(X_scaled)
            self.lof.fit(X_scaled)
            
            self.save_models()
        except Exception as e:
            trace.log_event(
                "ML_MODEL_TRAIN",
                "ERROR",
                {
                    "error": str(e)
                }
            )
            raise
    
    @trace.trace_operation
    def predict_behavior(self, features: Dict) -> str:
        """Prediz o comportamento com base nos recursos"""
        try:
            feature_vector = np.array([
                features['tx_frequency'],
                features['profit_margin'],
                features['holding_time'],
                features['rug_pull_score'],
                features['wash_trade_score']
            ]).reshape(1, -1)
            
            feature_vector = self.scaler.transform(feature_vector)
            
            # Predição com Isolation Forest
            if_pred = self.model.predict(feature_vector)
            
            # Predição com LOF
            lof_score = self.lof.negative_outlier_factor_
            
            # Clusterização com DBSCAN
            cluster = self.dbscan.fit_predict(feature_vector)
            
            if if_pred[0] == -1 or lof_score[0] < -1.5 or cluster[0] == -1:
                return "suspicious"
            return "normal"
        except Exception as e:
            trace.log_event(
                "ML_PREDICTION",
                "ERROR",
                {
                    "error": str(e)
                }
            )
            return "unknown"
    
    @trace.trace_operation
    def classify_wallet(self, wallet_history: Dict) -> str:
        """Classifica uma carteira com base em seu histórico"""
        # Verificar se precisa retreinar
        if time.time() - self.last_trained > self.config.retrain_interval:
            self._retrain_with_new_data()
        
        features = self.extract_features(wallet_history)
        behavior = self.predict_behavior(features)
        
        if behavior == "suspicious":
            if features['rug_pull_score'] > 0.8:
                return "rugger"
            if features['wash_trade_score'] > 0.7:
                return "wash_trader"
            if features['profit_margin'] > 0.5 and features['tx_frequency'] > 10:
                return "smart_money"
            return "suspicious"
        
        if features['holding_time'] > 30 and features['tx_frequency'] < 3:
            return "whale"
        
        if features['tx_frequency'] > 20 and features['profit_margin'] > 0.3:
            return "trader"
        
        return "normal"
    
    def _retrain_with_new_data(self):
        """Retreina o modelo com dados mais recentes"""
        # Implementação simplificada - em produção, buscaria novos dados
        try:
            # Carregar dados históricos
            data_dir = ModuleConfig().data_dir
            wallet_files = [f for f in os.listdir(data_dir) if f.startswith('wallet_')]
            
            features = []
            for wallet_file in wallet_files[:1000]:  # Limitar para demonstração
                with open(os.path.join(data_dir, wallet_file), 'r') as f:
                    wallet_data = json.load(f)
                    features.append(self.extract_features(wallet_data))
            
            if features:
                X = np.array([list(f.values()) for f in features])
                self.train_model(X)
        except Exception as e:
            trace.log_event(
                "ML_MODEL_RETRAIN",
                "ERROR",
                {
                    "error": str(e)
                }
            )
    
    @trace.trace_operation
    def extract_features(self, wallet_history: Dict) -> Dict:
        """Extrai recursos de um histórico de carteira"""
        txs = wallet_history.get('transactions', [])
        swaps = wallet_history.get('swaps', [])
        holdings = wallet_history.get('holdings', {})
        
        features = {
            'tx_frequency': len(txs),
            'profit_margin': self._calculate_profit_margin(swaps),
            'holding_time': self._calculate_avg_holding_time(holdings),
            'rug_pull_score': self._calculate_rug_pull_score(wallet_history),
            'wash_trade_score': self._calculate_wash_trade_score(swaps)
        }
        
        return features
    
    def _calculate_profit_margin(self, swaps: List[Dict]) -> float:
        """Calcula a margem média de lucro"""
        if not swaps:
            return 0.0
        
        profits = []
        for swap in swaps:
            if 'profit' in swap and swap['profit']:
                profits.append(swap['profit'])
        
        return np.mean(profits) if profits else 0.0
    
    def _calculate_avg_holding_time(self, holdings: Dict) -> float:
        """Calcula o tempo médio de retenção"""
        if not holdings:
            return 0.0
        
        holding_times = []
        for token, history in holdings.items():
            if 'buy_time' in history and 'sell_time' in history:
                holding_times.append((history['sell_time'] - history['buy_time']).total_seconds() / 3600)
        
        return np.mean(holding_times) if holding_times else 0.0
    
    def _calculate_rug_pull_score(self, wallet_history: Dict) -> float:
        """Calcula um score de probabilidade de rug pull"""
        score = 0.0
        
        # Check if deployer
        if wallet_history.get('is_deployer', False):
            score += 0.3
        
        # Check for liquidity removal
        if wallet_history.get('liquidity_removed', False):
            score += 0.4
        
        # Check for token dumping
        if wallet_history.get('tokens_dumped', False):
            score += 0.3
        
        return min(1.0, score)
    
    def _calculate_wash_trade_score(self, swaps: List[Dict]) -> float:
        """Calcula um score de probabilidade de wash trading"""
        if len(swaps) < 3:
            return 0.0
        
        amounts = [max(float(s.get('amount0_in', 0)), float(s.get('amount1_in', 0))) for s in swaps]
        avg_amount = np.mean(amounts)
        std_amount = np.std(amounts)
        
        if std_amount == 0:
            return 0.0
        
        z_scores = np.abs((amounts - avg_amount) / std_amount)
        return float(np.mean(z_scores > 3) * 0.5)
    
    @trace.trace_operation
    def detect_pump_and_dump(self, token_history: Dict) -> Dict:
        """Detecta padrões de pump and dump"""
        swaps = token_history.get('swaps', [])
        if len(swaps) < 10:
            return {
                'score': 0.0,
                'confidence': 'low',
                'indicators': []
            }
        
        # Calcular volume e preço ao longo do tempo
        timestamps = []
        prices = []
        volumes = []
        
        for swap in swaps:
            block = swap.get('blockNumber')
            if block:
                timestamp = swap.get('timestamp', 0)
                amount_in = max(float(swap.get('amount0_in', 0)), float(swap.get('amount1_in', 0)))
                amount_out = max(float(swap.get('amount0_out', 0)), float(swap.get('amount1_out', 0)))
                
                if amount_in > 0 and amount_out > 0:
                    price = amount_out / amount_in
                    volume = max(amount_in, amount_out)
                    
                    timestamps.append(timestamp)
                    prices.append(price)
                    volumes.append(volume)
        
        if len(prices) < 5:
            return {
                'score': 0.0,
                'confidence': 'low',
                'indicators': []
            }
        
        # Normalizar dados
        norm_prices = (prices - np.min(prices)) / (np.max(prices) - np.min(prices))
        norm_volumes = (volumes - np.min(volumes)) / (np.max(volumes) - np.min(volumes))
        
        # Calcular correlação preço-volume
        correlation = np.corrcoef(norm_prices, norm_volumes)[0, 1]
        
        # Calcular taxa de crescimento do preço
        price_growth = (norm_prices[-1] - norm_prices[0]) / len(norm_prices)
        
        # Calcular queda após pico
        max_idx = np.argmax(norm_prices)
        if max_idx < len(norm_prices) - 1:
            drop_after_peak = (norm_prices[max_idx] - norm_prices[-1]) / norm_prices[max_idx]
        else:
            drop_after_peak = 0.0
        
        # Calcular score (0-1)
        score = min(1.0, max(0.0, 
            (0.4 * correlation) + 
            (0.3 * price_growth) + 
            (0.3 * drop_after_peak)
        ))
        
        indicators = []
        if correlation > 0.7:
            indicators.append(f"high_price_volume_correlation ({correlation:.2f})")
        if price_growth > 0.5:
            indicators.append(f"rapid_price_growth ({price_growth:.2f})")
        if drop_after_peak > 0.5:
            indicators.append(f"sharp_price_drop ({drop_after_peak:.2f})")
        
        return {
            'score': score,
            'confidence': 'high' if score > 0.7 else 'medium' if score > 0.5 else 'low',
            'indicators': indicators
        }

# ==============================================
