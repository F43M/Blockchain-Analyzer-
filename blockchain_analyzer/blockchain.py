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
from .config import ModuleConfig, ModuleStatus
from .utils import Utils, trace

# ==============================================
# Módulo: Conexão Blockchain
# ==============================================

class BlockchainConnector:
    """Gerenciador de conexões multi-blockchain com failover"""
    
    def __init__(self, chain: str):
        self.chain = chain
        self.config = ModuleConfig()
        self.chain_config = self.config.blockchains.get(chain)
        
        if not self.chain_config or not self.chain_config.enabled:
            raise ValueError(f"Blockchain {chain} não está configurada ou habilitada")
        
        self.current_endpoint_idx = 0
        self.endpoint_failures = defaultdict(int)
        self.max_failures = len(self.chain_config.rpc_endpoints) * 2
        self.lock = asyncio.Lock()
        
        # Pool de conexões
        self.w3_pool = []
        self.async_w3_pool = []
        self._initialize_pool()
        
        # Middlewares
        self.poa_middleware = geth_poa_middleware
        
        # Caches
        self._abi_cache = {}
        self._contract_cache = {}
    
    def _initialize_pool(self):
        """Inicializa o pool de conexões"""
        for _ in range(self.config.max_connections):
            w3 = Web3(Web3.HTTPProvider(
                self.chain_config.rpc_endpoints[self.current_endpoint_idx],
                request_kwargs={'timeout': self.config.timeout}
            ))
            
            async_w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(
                self.chain_config.rpc_endpoints[self.current_endpoint_idx],
                request_kwargs={'timeout': self.config.timeout}
            ))
            
            if self.chain in ['bsc', 'polygon', 'avalanche']:
                w3.middleware_onion.inject(self.poa_middleware, layer=0)
                async_w3.middleware_onion.inject(self.poa_middleware, layer=0)
            
            self.w3_pool.append(w3)
            self.async_w3_pool.append(async_w3)
    
    async def _rotate_endpoint(self):
        """Rotaciona para o próximo endpoint RPC"""
        async with self.lock:
            self.current_endpoint_idx = (self.current_endpoint_idx + 1) % len(self.chain_config.rpc_endpoints)
            self._initialize_pool()
    
    async def _handle_failure(self, endpoint: str):
        """Registra falha e rotaciona se necessário"""
        self.endpoint_failures[endpoint] += 1
        self.config.rpc_failures[self.chain] += 1
        
        if self.endpoint_failures[endpoint] > 3:
            await self._rotate_endpoint()
        
        if self.config.rpc_failures[self.chain] > self.max_failures:
            self.config.status = ModuleStatus.DEGRADED
    
    async def _execute_with_retry(self, func, *args, **kwargs):
        """Executa uma função com retry automático"""
        last_error = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries:
                    delay = Utils.exponential_backoff(attempt, self.config.retry_delay)
                    await asyncio.sleep(delay)
                    await self._rotate_endpoint()
        
        trace.log_event(
            "BLOCKCHAIN_OPERATION",
            "ERROR",
            {
                "chain": self.chain,
                "operation": func.__name__,
                "error": str(last_error),
                "retries": self.config.max_retries
            }
        )
        raise last_error
    
    @trace.trace_operation
    async def is_connected(self) -> bool:
        """Verifica se a conexão está ativa"""
        try:
            w3 = self.async_w3_pool[0]
            return await w3.is_connected()
        except Exception as e:
            await self._handle_failure(self.chain_config.rpc_endpoints[self.current_endpoint_idx])
            raise
    
    @trace.trace_operation
    async def get_block(self, block_identifier: Union[int, str] = 'latest') -> BlockData:
        """Obtém informações de um bloco"""
        async def _get_block():
            w3 = self.async_w3_pool[0]
            return await w3.eth.get_block(block_identifier)
        
        return await self._execute_with_retry(_get_block)
    
    @trace.trace_operation
    async def get_transaction(self, tx_hash: str) -> Dict:
        """Obtém uma transação pelo hash"""
        async def _get_transaction():
            w3 = self.async_w3_pool[0]
            return await w3.eth.get_transaction(tx_hash)
        
        return await self._execute_with_retry(_get_transaction)
    
    @trace.trace_operation
    async def get_transaction_receipt(self, tx_hash: str) -> TxReceipt:
        """Obtém o recibo de uma transação"""
        async def _get_receipt():
            w3 = self.async_w3_pool[0]
            return await w3.eth.get_transaction_receipt(tx_hash)
        
        return await self._execute_with_retry(_get_receipt)
    
    @trace.trace_operation
    async def get_contract(self, address: str, abi: Optional[List[Dict]] = None) -> Any:
        """Obtém um contrato"""
        address = Web3.to_checksum_address(address)
        
        if address in self._contract_cache:
            return self._contract_cache[address]
        
        if abi is None:
            abi = await self.get_contract_abi(address)
            if abi is None:
                raise ValueError(f"ABI não encontrada para o contrato {address}")
        
        contract = self.async_w3_pool[0].eth.contract(address=address, abi=abi)
        self._contract_cache[address] = contract
        return contract
    
    @trace.trace_operation
    async def get_contract_abi(self, address: str) -> Optional[List[Dict]]:
        """Obtém a ABI de um contrato"""
        if address in self._abi_cache:
            return self._abi_cache[address]
        
        cache_key = f"{self.chain}_{address}_abi"
        cached_abi = await self.config.get_cache(cache_key)
        if cached_abi:
            self._abi_cache[address] = cached_abi
            return cached_abi
        
        abi = await self._fetch_abi_from_scan(address)
        if abi:
            self._abi_cache[address] = abi
            await self.config.set_cache(cache_key, abi)
            return abi
        
        return None
    
    async def _fetch_abi_from_scan(self, address: str) -> Optional[List[Dict]]:
        """Tenta identificar uma ABI padrão via RPC.

        Caso não seja possível determinar o ABI completo, retorna uma
        estrutura mínima contendo apenas as funções ``name``, ``symbol`` e
        ``decimals`` para interagir com tokens ERC‑20 compatíveis.
        """

        minimal_abi = [
            {
                "constant": True,
                "inputs": [],
                "name": "name",
                "outputs": [{"name": "", "type": "string"}],
                "type": "function",
            },
            {
                "constant": True,
                "inputs": [],
                "name": "symbol",
                "outputs": [{"name": "", "type": "string"}],
                "type": "function",
            },
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function",
            },
        ]

        w3 = self.async_w3_pool[0]
        contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=minimal_abi)

        try:
            # Testa se o contrato responde às chamadas padrão
            await contract.functions.name().call()
            await contract.functions.symbol().call()
            await contract.functions.decimals().call()
            return minimal_abi
        except Exception:
            # Mesmo que as chamadas falhem, retorna o ABI mínimo para permitir
            # interações básicas quando possível
            return minimal_abi
    
    @trace.trace_operation
    async def listen_for_pending_transactions(self, callback: callable):
        """Escuta por transações pendentes"""
        endpoint = self.chain_config.rpc_endpoints[self.current_endpoint_idx].replace('http', 'ws')
        
        while True:
            try:
                async with connect(endpoint) as ws:
                    await ws.send(json.dumps({
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": ["newPendingTransactions"]
                    }))
                    
                    subscription_response = await ws.recv()
                    
                    while True:
                        message = await asyncio.wait_for(ws.recv(), timeout=60)
                        message = json.loads(message)
                        tx_hash = message['params']['result']
                        
                        try:
                            tx = await self.get_transaction(tx_hash)
                            callback(tx)
                        except Exception as e:
                            trace.log_event(
                                "PENDING_TX_PROCESSING",
                                "ERROR",
                                {
                                    "tx_hash": tx_hash,
                                    "error": str(e)
                                }
                            )
            except Exception as e:
                trace.log_event(
                    "PENDING_TX_LISTENER",
                    "ERROR",
                    {
                        "endpoint": endpoint,
                        "error": str(e)
                    }
                )
                await asyncio.sleep(5)
                await self._rotate_endpoint()

# ==============================================
# Módulo: Alertas
# ==============================================

class AlertSystem:
    """Sistema de alertas multi-canal"""
    
    @staticmethod
    @trace.trace_operation
    def send_alert(message: str, alert_type: str = 'info', data: Optional[Dict] = None):
        """Envia alertas para todos os canais configurados"""
        config = ModuleConfig().alerts
        full_message = f"[{alert_type.upper()}] {message}"
        
        if data:
            full_message += f"\n\nDados:\n{json.dumps(data, indent=2)}"
        
        # Discord
        if config.discord_webhook:
            try:
                webhook = DiscordWebhook(url=config.discord_webhook, content=full_message[:2000])
                webhook.execute()
            except Exception as e:
                trace.log_event(
                    "ALERT_DISCORD",
                    "ERROR",
                    {
                        "error": str(e),
                        "message": message[:100]
                    }
                )
        
        # Telegram
        if config.telegram_bot_token and config.telegram_chat_id:
            try:
                bot = telegram.Bot(token=config.telegram_bot_token)
                bot.send_message(chat_id=config.telegram_chat_id, text=full_message[:4096])
            except Exception as e:
                trace.log_event(
                    "ALERT_TELEGRAM",
                    "ERROR",
                    {
                        "error": str(e),
                        "message": message[:100]
                    }
                )
        
        # Email
        if config.email_alerts['enabled']:
            try:
                msg = MIMEText(full_message)
                msg['Subject'] = f"[{alert_type.upper()}] Blockchain Alert"
                msg['From'] = config.email_alerts['email_from']
                msg['To'] = config.email_alerts['email_to']
                
                with smtplib.SMTP(config.email_alerts['smtp_server'], config.email_alerts['smtp_port']) as server:
                    server.starttls()
                    server.login(config.email_alerts['email_from'], config.email_alerts['email_password'])
                    server.send_message(msg)
            except Exception as e:
                trace.log_event(
                    "ALERT_EMAIL",
                    "ERROR",
                    {
                        "error": str(e),
                        "message": message[:100]
                    }
                )
    
    @staticmethod
    def format_alert(event_type: str, chain: str, data: Dict) -> str:
        """Formata um alerta de acordo com o tipo de evento"""
        if event_type == "WHALE_ACTIVITY":
            return (
                f"🐳 Atividade de Baleia Detectada em {chain.upper()} 🐳\n"
                f"Endereço: {data.get('address')}\n"
                f"Token: {data.get('token')}\n"
                f"Valor: {data.get('amount')} {data.get('symbol', 'ETH')}\n"
                f"Tipo: {data.get('direction', 'buy')}"
            )
        elif event_type == "HIGH_RISK":
            return (
                f"🚨 ALTO RISCO Detectado em {chain.upper()} 🚨\n"
                f"Token: {data.get('token_address')}\n"
                f"Pontuação de Risco: {data.get('risk_score')}/10\n"
                f"Fatores: {', '.join(data.get('risk_factors', []))[:200]}"
            )
        elif event_type == "SUSPICIOUS_DEPLOYER":
            return (
                f"⚠️ Implementador Suspeito em {chain.upper()} ⚠️\n"
                f"Token: {data.get('token_address')}\n"
                f"Implementador: {data.get('deployer_address')}\n"
                f"Riscos: {', '.join(data.get('risk_indicators', []))}"
            )
        else:
            return f"Evento {event_type} em {chain.upper()}: {json.dumps(data, indent=2)}"

# ==============================================
