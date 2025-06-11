import os
import json
import asyncio
from typing import AsyncGenerator, Optional

from .blockchain import BlockchainConnector


class BlockScanner:
    """Classe utilitária para escanear blocos da blockchain sequencialmente e persistir o estado."""

    def __init__(self, connector: BlockchainConnector, state_file: str = "data/scan_state.json"):
        self.connector = connector
        self.state_file = state_file
        self._lock = asyncio.Lock()

    async def get_saved_block(self) -> int:
        """Retorna o último bloco salvo para a cadeia atual ou 0 se não houver."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    return int(data.get(self.connector.chain, 0))
            except Exception:
                return 0
        return 0

    async def _save_block(self, block_number: int) -> None:
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        async with self._lock:
            state = {}
            if os.path.exists(self.state_file):
                try:
                    with open(self.state_file, "r") as f:
                        state = json.load(f)
                except Exception:
                    state = {}
            state[self.connector.chain] = block_number
            with open(self.state_file, "w") as f:
                json.dump(state, f)

    async def scan(self, start_block: Optional[int] = None) -> AsyncGenerator[dict, None]:
        """Itera sobre os blocos a partir de ``start_block`` até o mais recente."""
        if start_block is None:
            start_block = await self.get_saved_block()
        latest = await self.connector.async_w3.eth.block_number
        for block_num in range(start_block, latest + 1):
            block = await self.connector.get_block(block_num)
            yield block
            await self._save_block(block_num)
