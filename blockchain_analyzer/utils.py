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
from .config import ModuleIdentity, ModuleStatus

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
