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

# ==============================================
# Módulo: ModuleIdentity
# ==============================================

class ModuleIdentity:
    """Identidade e metadados do módulo conforme padrão SCLP"""
    
    MODULE_ID = "blockchain_analyzer"
    VERSION = "2.0.0"
    DESCRIPTION = "Análise avançada de blockchain com detecção de riscos, classificação de carteiras e monitoramento em tempo real"
    AUTHOR = "Seu Nome"
    LICENSE = "MIT"
    SCLP_CONTRACT = "SCLP-1.0"
    COMPATIBILITY = {
        "python": ">=3.8",
        "web3.py": ">=6.0.0",
        "scikit-learn": ">=1.0.0"
    }
    
    @classmethod
    def get_identity(cls) -> Dict:
        """Retorna a identidade completa do módulo"""
        return {
            "module": cls.MODULE_ID,
            "version": cls.VERSION,
            "description": cls.DESCRIPTION,
            "author": cls.AUTHOR,
            "license": cls.LICENSE,
            "sclp_contract": cls.SCLP_CONTRACT,
            "compatibility": cls.COMPATIBILITY,
            "timestamp": datetime.now(pytz.utc).isoformat()
        }

# ==============================================
# Módulo: Configurações
# ==============================================

class ModuleStatus(Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"

@dataclass
class BlockchainConfig:
    name: str
    rpc_endpoints: List[str]
    scan_api: Optional[str] = None
    scan_api_key: Optional[str] = None
    chain_id: Optional[int] = None
    priority: int = 1
    enabled: bool = True

@dataclass
class AlertConfig:
    discord_webhook: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    email_alerts: Dict = field(default_factory=lambda: {
        'enabled': False,
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'email_from': '',
        'email_password': '',
        'email_to': ''
    })

@dataclass
class RiskThresholds:
    whale_threshold_eth: float = 10.0
    min_swap_amount_eth: float = 1.0
    risk_threshold: float = 0.1
    early_adopter_window: int = 100
    high_risk_score: float = 7.0
    moderate_risk_score: float = 4.0

@dataclass
class MLConfig:
    contamination: float = 0.1
    random_state: int = 42
    model_path: str = "ml_models/behavior_model.joblib"
    scaler_path: str = "ml_models/behavior_scaler.joblib"
    retrain_interval: int = 86400  # 24 horas em segundos

class ModuleConfig:
    """Configuração centralizada do módulo com carregamento de YAML"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        self.config_path = "blockchain_analyzer/module_config.yaml"
        self.fernet_key = os.getenv('CONFIG_ENCRYPTION_KEY', 'default_key_that_should_be_changed')
        self._validate_fernet_key()
        self._load_config()
        self._setup_directories()
        self.status = ModuleStatus.OK
        self.last_health_check = datetime.now(pytz.utc)
        self.rpc_failures = defaultdict(int)
        self.cache = {}
        self.cache_lock = Lock()
        self.redis_client = None
        self.redis_enabled = False
        
    def _validate_fernet_key(self):
        if len(self.fernet_key) < 32:
            self.fernet_key = self.fernet_key.ljust(32, '0')[:32]
        self.fernet = cryptography.fernet.Fernet(self.fernet_key.encode())
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f) or {}
            
            # Configurações de blockchain
            self.blockchains = {}
            for chain, data in config_data.get('blockchains', {}).items():
                self.blockchains[chain] = BlockchainConfig(
                    name=chain,
                    rpc_endpoints=data.get('rpc_endpoints', []),
                    scan_api=data.get('scan_api'),
                    scan_api_key=data.get('scan_api_key'),
                    chain_id=data.get('chain_id'),
                    priority=data.get('priority', 1),
                    enabled=data.get('enabled', True)
                )
            
            # Configurações de alerta
            alerts = config_data.get('alerts', {})
            self.alerts = AlertConfig(
                discord_webhook=alerts.get('discord_webhook'),
                telegram_bot_token=alerts.get('telegram_bot_token'),
                telegram_chat_id=alerts.get('telegram_chat_id'),
                email_alerts=alerts.get('email_alerts', {})
            )
            
            # Limiares de risco
            thresholds = config_data.get('risk_thresholds', {})
            self.thresholds = RiskThresholds(
                whale_threshold_eth=thresholds.get('whale_threshold_eth', 10.0),
                min_swap_amount_eth=thresholds.get('min_swap_amount_eth', 1.0),
                risk_threshold=thresholds.get('risk_threshold', 0.1),
                early_adopter_window=thresholds.get('early_adopter_window', 100),
                high_risk_score=thresholds.get('high_risk_score', 7.0),
                moderate_risk_score=thresholds.get('moderate_risk_score', 4.0)
            )
            
            # Configurações de ML
            ml = config_data.get('machine_learning', {})
            self.ml_config = MLConfig(
                contamination=ml.get('contamination', 0.1),
                random_state=ml.get('random_state', 42),
                model_path=ml.get('model_path', "ml_models/behavior_model.joblib"),
                scaler_path=ml.get('scaler_path', "ml_models/behavior_scaler.joblib"),
                retrain_interval=ml.get('retrain_interval', 86400)
            )
            
            # Caminhos de dados
            self.data_dir = config_data.get('data_dir', 'token_data')
            self.cache_dir = config_data.get('cache_dir', 'cache')
            self.models_dir = config_data.get('models_dir', 'ml_models')
            
            # Configurações de desempenho
            perf = config_data.get('performance', {})
            self.max_retries = perf.get('max_retries', 3)
            self.retry_delay = perf.get('retry_delay', 1.0)
            self.timeout = perf.get('timeout', 30.0)
            self.max_connections = perf.get('max_connections', 10)
            self.cache_ttl = perf.get('cache_ttl', 3600)
            
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {str(e)}")
    
    def _setup_directories(self):
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.models_dir, exist_ok=True)
    
    async def connect_redis(self):
        """Conecta ao Redis se configurado"""
        try:
            if not self.redis_enabled and hasattr(self, 'redis_url'):
                self.redis_client = await aioredis.from_url(self.redis_url)
                self.redis_enabled = True
                return True
        except Exception as e:
            logging.warning(f"Failed to connect to Redis: {str(e)}")
            self.redis_enabled = False
        return False
    
    async def get_cache(self, key: str) -> Optional[Any]:
        """Obtém um valor do cache"""
        try:
            if self.redis_enabled and self.redis_client:
                cached = await self.redis_client.get(key)
                if cached:
                    return json.loads(cached)
            elif key in self.cache:
                if self.cache[key]['expiry'] > time.time():
                    return self.cache[key]['value']
                del self.cache[key]
        except Exception as e:
            logging.warning(f"Cache get failed: {str(e)}")
        return None
    
    async def set_cache(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Define um valor no cache"""
        ttl = ttl or self.cache_ttl
        try:
            if self.redis_enabled and self.redis_client:
                await self.redis_client.set(key, json.dumps(value), ex=ttl)
                return True
            else:
                with self.cache_lock:
                    self.cache[key] = {
                        'value': value,
                        'expiry': time.time() + ttl
                    }
                return True
        except Exception as e:
            logging.warning(f"Cache set failed: {str(e)}")
            return False
    
    def update_config(self, new_config: Dict) -> bool:
        """Atualiza a configuração do módulo dinamicamente"""
        try:
            with open(self.config_path, 'w') as f:
                yaml.safe_dump(new_config, f)
            self._load_config()
            return True
        except Exception as e:
            logging.error(f"Failed to update config: {str(e)}")
            return False
    
    def health_check(self) -> Dict:
        """Verifica a saúde do módulo"""
        now = datetime.now(pytz.utc)
        health = {
            "module": ModuleIdentity.MODULE_ID,
            "version": ModuleIdentity.VERSION,
            "status": self.status.value,
            "timestamp": now.isoformat(),
            "uptime": (now - self.last_health_check).total_seconds(),
            "details": {
                "blockchains": {chain: {
                    "enabled": cfg.enabled,
                    "rpc_endpoints": len(cfg.rpc_endpoints),
                    "priority": cfg.priority
                } for chain, cfg in self.blockchains.items()},
                "alerts": {
                    "discord": bool(self.alerts.discord_webhook),
                    "telegram": bool(self.alerts.telegram_bot_token and self.alerts.telegram_chat_id),
                    "email": self.alerts.email_alerts['enabled']
                },
                "cache": {
                    "enabled": True,
                    "type": "redis" if self.redis_enabled else "memory",
                    "size": len(self.cache) if not self.redis_enabled else "unknown"
                }
            }
        }
        
        # Verificar status dos RPCs
        degraded_chains = []
        for chain, failures in self.rpc_failures.items():
            if failures > len(self.blockchains[chain].rpc_endpoints) * 0.5:
                degraded_chains.append(chain)
        
        if degraded_chains:
            self.status = ModuleStatus.DEGRADED
            health["status"] = self.status.value
            health["degraded_chains"] = degraded_chains
        
        return health

# ==============================================
# Módulo: Validação
# ==============================================

class Validator:
    """Validador de entradas e saídas do módulo"""
    
    @staticmethod
    def validate_address(address: str) -> bool:
        """Valida um endereço de blockchain"""
        if not address or not isinstance(address, str):
            return False
        return Web3.is_address(address)
    
    @staticmethod
    def validate_tx_hash(tx_hash: str) -> bool:
        """Valida um hash de transação"""
        if not tx_hash or not isinstance(tx_hash, str):
            return False
        return len(tx_hash) == 66 and tx_hash.startswith('0x')
    
    @staticmethod
    def validate_block_number(block: Union[int, str]) -> bool:
        """Valida um número de bloco"""
        if isinstance(block, str):
            if block == 'latest' or block == 'earliest' or block == 'pending':
                return True
            try:
                block = int(block)
            except ValueError:
                return False
        
        return isinstance(block, int) and block >= 0
    
    @staticmethod
    def validate_output(output: Dict) -> bool:
        """Valida a estrutura de saída padrão do módulo"""
        required_keys = {'status', 'module', 'blockchain', 'metrics', 'timestamp'}
        if not all(key in output for key in required_keys):
            return False
        
        if output['module'] != ModuleIdentity.MODULE_ID:
            return False
        
        if output['status'] not in [s.value for s in ModuleStatus]:
            return False
        
        try:
            datetime.fromisoformat(output['timestamp'])
        except ValueError:
            return False
        
        return True
    
    @staticmethod
    def validate_config(config: Dict) -> bool:
        """Valida uma configuração do módulo"""
        required_sections = {'blockchains', 'alerts', 'risk_thresholds'}
        if not all(section in config for section in required_sections):
            return False
        
        for chain, data in config['blockchains'].items():
            if not isinstance(chain, str):
                return False
            if not isinstance(data.get('rpc_endpoints', []), list):
                return False
            if not all(isinstance(url, str) for url in data.get('rpc_endpoints', [])):
                return False
        
        return True

# ==============================================
# Módulo: Rastreamento e Logs
# ==============================================

class DebugTrace:
    """Sistema de rastreamento e logging estruturado"""
    
    def __init__(self):
        self.logger = logging.getLogger(ModuleIdentity.MODULE_ID)
        self.logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo
        file_handler = logging.FileHandler('blockchain_analyzer/debug.log')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        self.event_buffer = deque(maxlen=1000)
    
    def log_event(self, event: str, status: str, details: Dict = None):
        """Registra um evento estruturado"""
        log_entry = {
            "timestamp": datetime.now(pytz.utc).isoformat(),
            "module": ModuleIdentity.MODULE_ID,
            "event": event,
            "status": status,
            "details": details or {}
        }
        
        self.event_buffer.append(log_entry)
        
        if status == "ERROR":
            self.logger.error(json.dumps(log_entry))
        elif status == "WARNING":
            self.logger.warning(json.dumps(log_entry))
        else:
            self.logger.info(json.dumps(log_entry))
    
    def get_recent_events(self, limit: int = 100) -> List[Dict]:
        """Retorna os eventos recentes"""
        return list(self.event_buffer)[-limit:]
    
    def trace_operation(self, func):
        """Decorator para rastrear operações"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            event_name = f"{func.__module__}.{func.__name__}"
            
            try:
                result = await func(*args, **kwargs)
                self.log_event(
                    event=event_name,
                    status="SUCCESS",
                    details={
                        "duration": time.time() - start_time,
                        "args": str(args),
                        "kwargs": str(kwargs)
                    }
                )
                return result
            except Exception as e:
                self.log_event(
                    event=event_name,
                    status="ERROR",
                    details={
                        "error": str(e),
                        "duration": time.time() - start_time,
                        "args": str(args),
                        "kwargs": str(kwargs)
                    }
                )
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            event_name = f"{func.__module__}.{func.__name__}"
            
            try:
                result = func(*args, **kwargs)
                self.log_event(
                    event=event_name,
                    status="SUCCESS",
                    details={
                        "duration": time.time() - start_time,
                        "args": str(args),
                        "kwargs": str(kwargs)
                    }
                )
                return result
            except Exception as e:
                self.log_event(
                    event=event_name,
                    status="ERROR",
                    details={
                        "error": str(e),
                        "duration": time.time() - start_time,
                        "args": str(args),
                        "kwargs": str(kwargs)
                    }
                )
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

# Inicializa o sistema de rastreamento
trace = DebugTrace()

# ==============================================
# Módulo: Utilitários
# ==============================================

class Utils:
    """Coleção de utilitários para o módulo"""
    
    @staticmethod
    def calculate_checksum(data: Dict) -> str:
        """Calcula um checksum SHA256 para os dados"""
        data_str = json.dumps(data, sort_keys=True)
        return f"SHA256:{hashlib.sha256(data_str.encode()).hexdigest()}"
    
    @staticmethod
    def generate_sclp_code(data: Dict) -> Dict:
        """Converte os dados para o formato simbólico SCLP"""
        sclp_data = {
            "header": {
                "module": ModuleIdentity.MODULE_ID,
                "version": ModuleIdentity.VERSION,
                "contract": ModuleIdentity.SCLP_CONTRACT,
                "timestamp": datetime.now(pytz.utc).isoformat()
            },
            "payload": data,
            "validation": {
                "checksum": Utils.calculate_checksum(data),
                "valid": True
            }
        }
        return sclp_data
    
    @staticmethod
    def exponential_backoff(retries: int, base_delay: float = 1.0, max_delay: float = 30.0) -> float:
        """Calcula um delay exponencial para retry"""
        delay = min(max_delay, base_delay * (2 ** (retries - 1)))
        return delay + np.random.uniform(0, 0.1 * delay)
    
    @staticmethod
    async def fetch_with_retry(session, url: str, retries: int = 3, **kwargs) -> Optional[Dict]:
        """Fetch com retry exponencial"""
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, **kwargs) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status >= 500:
                        raise aiohttp.ClientError(f"Server error: {response.status}")
                    else:
                        raise aiohttp.ClientError(f"Request failed: {response.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < retries:
                    delay = Utils.exponential_backoff(attempt)
                    await asyncio.sleep(delay)
        
        trace.log_event(
            "HTTP_REQUEST",
            "ERROR",
            {
                "url": url,
                "error": str(last_error),
                "retries": retries
            }
        )
        return None
    
    @staticmethod
    def normalize_address(address: str) -> str:
        """Normaliza um endereço para checksum"""
        try:
            return Web3.to_checksum_address(address)
        except ValueError:
            return address.lower()
    
    @staticmethod
    def wei_to_eth(wei: int) -> float:
        """Converte wei para ETH"""
        return wei / 10**18
    
    @staticmethod
    def calculate_gini_coefficient(values: List[float]) -> float:
        """Calcula o coeficiente de Gini para distribuição"""
        values = sorted(values)
        n = len(values)
        if n == 0:
            return 0.0
        
        cum_values = np.cumsum(values).astype(float)
        gini = (n + 1 - 2 * np.sum(cum_values) / cum_values[-1]) / n
        return float(gini)

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
        """Busca a ABI de um contrato usando a API do scan"""
        if not self.chain_config.scan_api or not self.chain_config.scan_api_key:
            return None
        
        url = f"{self.chain_config.scan_api}?module=contract&action=getabi&address={address}&apikey={self.chain_config.scan_api_key}"
        
        async with aiohttp.ClientSession() as session:
            data = await Utils.fetch_with_retry(session, url)
            if data and data.get('status') == '1':
                return json.loads(data['result'])
        
        return None
    
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
        """Carrega endereços de scammers conhecidos"""
        try:
            response = requests.get("https://raw.githubusercontent.com/scam-alert/scam-addresses/main/multichain.json")
            if response.status_code == 200:
                data = response.json()
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
        
        trades = []
        for pool_address in pools:
            try:
                pool_contract = await self.connector.get_contract(pool_address)
                swap_events = await pool_contract.events.Swap.get_logs(fromBlock='earliest')
                
                for event in swap_events:
                    tx_receipt = await self.connector.get_transaction_receipt(event['transactionHash'])
                    block = await self.connector.get_block(event['blockNumber'])
                    
                    if token_address.lower() == event['args']['token0'].lower():
                        amount = int(event['args']['amount0In']) - int(event['args']['amount0Out'])
                        price = abs(int(event['args']['amount1Out']) / int(event['args']['amount0In'])) if int(event['args']['amount0In']) > 0 else None
                    else:
                        amount = int(event['args']['amount1In']) - int(event['args']['amount1Out'])
                        price = abs(int(event['args']['amount0Out']) / int(event['args']['amount1In'])) if int(event['args']['amount1In']) > 0 else None
                    
                    if price:
                        trades.append({
                            'from': tx_receipt['from'],
                            'to': tx_receipt['to'],
                            'amount': str(abs(amount)),
                            'price': float(price),
                            'timestamp': block['timestamp'],
                            'tx_hash': event['transactionHash'].hex()
                        })
            except Exception as e:
                trace.log_event(
                    "TRADE_FETCH",
                    "ERROR",
                    {
                        "pool": pool_address,
                        "error": str(e)
                    }
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
            # Primeiro verifica se já temos essa informação
            if token_address in self.deployers:
                return self.deployers[token_address]
            
            # Tenta obter via código do contrato
            code = await self.connector.async_w3.eth.get_code(token_address)
            if code:
                tx_hash = code['transactionHash']
                tx = await self.connector.get_transaction(tx_hash)
                return tx['from']
            
            # Se não conseguir, tenta via API do scan
            if self.connector.chain_config.scan_api:
                url = f"{self.connector.chain_config.scan_api}?module=contract&action=getcontractcreation&contractaddresses={token_address}&apikey={self.connector.chain_config.scan_api_key}"
                
                async with aiohttp.ClientSession() as session:
                    data = await Utils.fetch_with_retry(session, url)
                    if data and data.get('status') == '1' and data.get('result'):
                        return data['result'][0]['contractCreator']
            
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
        """Verifica outros contratos implementados pelo mesmo endereço"""
        if not self.connector.chain_config.scan_api or not self.connector.chain_config.scan_api_key:
            return []
        
        url = f"{self.connector.chain_config.scan_api}?module=account&action=txlist&address={deployer_address}&startblock=0&endblock=99999999&sort=asc&apikey={self.connector.chain_config.scan_api_key}"
        
        async with aiohttp.ClientSession() as session:
            data = await Utils.fetch_with_retry(session, url)
            if data and data.get('status') == '1':
                contracts = []
                for tx in data.get('result', []):
                    if tx.get('to') == '' and tx.get('isError') == '0':
                        contracts.append({
                            'contract_address': tx.get('contractAddress'),
                            'timestamp': int(tx.get('timeStamp', 0)),
                            'tx_hash': tx.get('hash')
                        })
                return contracts
        
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
        
        # Obter transações regulares
        if self.connector.chain_config.scan_api:
            url = f"{self.connector.chain_config.scan_api}?module=account&action=txlist&address={wallet_address}&startblock=0&endblock=99999999&sort=asc&apikey={self.connector.chain_config.scan_api_key}"
            
            async with aiohttp.ClientSession() as session:
                data = await Utils.fetch_with_retry(session, url)
                if data and data.get('status') == '1':
                    history['transactions'] = data.get('result', [])
        
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
# Módulo: Dashboard Web
# ==============================================

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Inicializar trackers para cada blockchain configurada
trackers = {}
for chain in ModuleConfig().blockchains:
    if ModuleConfig().blockchains[chain].enabled:
        try:
            trackers[chain] = MemeCoinTracker(chain)
        except Exception as e:
            trace.log_event(
                "TRACKER_INIT",
                "ERROR",
                {
                    "chain": chain,
                    "error": str(e)
                }
            )

@app.on_event("startup")
async def startup_event():
    """Inicia tarefas em segundo plano ao iniciar o aplicativo"""
    for chain, tracker in trackers.items():
        asyncio.create_task(tracker.start_mempool_monitoring())

@app.get("/")
async def dashboard():
    """Endpoint principal do dashboard"""
    return templates.TemplateResponse("dashboard.html", {"request": {}})

@app.get("/api/health")
async def health_check():
    """Endpoint de verificação de saúde"""
    return ModuleConfig().health_check()

@app.get("/api/token/{chain}/{token_address}")
async def get_token_data(chain: str, token_address: str):
    """Endpoint para obter dados de um token"""
    if chain not in trackers:
        raise HTTPException(status_code=404, detail="Chain not supported")
    
    try:
        data = await trackers[chain].track_token_contract(token_address)
        return Utils.generate_sclp_code(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/watchlist/{chain}")
async def get_watchlist(chain: str):
    """Endpoint para obter a lista de observação"""
    if chain not in trackers:
        raise HTTPException(status_code=404, detail="Chain not supported")
    
    try:
        watchlist = []
        for token_address in trackers[chain].watched_tokens:
            watchlist.append(await trackers[chain].generate_watchlist(token_address))
        
        return Utils.generate_sclp_code({"watchlist": watchlist})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/watch/{chain}/{address}")
async def watch_address(chain: str, address: str, address_type: str = 'token'):
    """Endpoint para adicionar um endereço à lista de observação"""
    if chain not in trackers:
        raise HTTPException(status_code=404, detail="Chain not supported")
    
    if address_type == 'token':
        trackers[chain].watch_token(address)
    else:
        trackers[chain].watch_wallet(address)
    
    return {"status": "success"}

@app.get("/api/risk/{chain}/{token_address}")
async def get_risk_report(chain: str, token_address: str):
    """Endpoint para obter relatório de risco"""
    if chain not in trackers:
        raise HTTPException(status_code=404, detail="Chain not supported")
    
    try:
        report = await trackers[chain].generate_risk_report(token_address)
        return Utils.generate_sclp_code(report)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def run_dashboard():
    """Executa o dashboard web"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

# ==============================================
# Execução Principal
# ==============================================

if __name__ == "__main__":
    # Iniciar dashboard em uma thread separada
    dashboard_thread = Thread(target=run_dashboard, daemon=True)
    dashboard_thread.start()
    
    # Exemplo de uso
    async def main():
        if 'bsc' in trackers:
            bsc_tracker = trackers['bsc']
            
            # Rastrear um token
            token_address = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"  # WBNB
            await bsc_tracker.track_token_contract(token_address)
            
            # Gerar lista de observação
            watchlist = await bsc_tracker.generate_watchlist(token_address)
            print(f"Watchlist gerada para {token_address}")
            
            # Gerar relatório de risco
            risk_report = await bsc_tracker.generate_risk_report(token_address)
            print(f"Pontuação de Risco: {risk_report['risk_score']}/10")
    
    asyncio.run(main())