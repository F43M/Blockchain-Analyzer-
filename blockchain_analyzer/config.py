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
