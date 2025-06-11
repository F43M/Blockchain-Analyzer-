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
from .tracker import MemeCoinTracker
from .utils import Utils, trace

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
