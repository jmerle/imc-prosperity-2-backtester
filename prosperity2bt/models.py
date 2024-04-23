import orjson
from dataclasses import dataclass
from prosperity2bt.datamodel import Trade
from typing import Any

@dataclass
class SandboxLogRow:
    timestamp: int
    sandbox_log: str
    lambda_log: str

    def with_offset(self, timestamp_offset: int) -> "SandboxLogRow":
        return SandboxLogRow(
            self.timestamp + timestamp_offset,
            self.sandbox_log,
            self.lambda_log.replace(f"[[{self.timestamp},", f"[[{self.timestamp + timestamp_offset},"),
        )

    def __str__(self) -> str:
        return orjson.dumps({
            "sandboxLog": self.sandbox_log,
            "lambdaLog": self.lambda_log,
            "timestamp": self.timestamp,
        }, option=orjson.OPT_APPEND_NEWLINE | orjson.OPT_INDENT_2).decode("utf-8")

@dataclass
class ActivityLogRow:
    columns: list[Any]

    @property
    def timestamp(self) -> int:
        return self.columns[1]

    def with_offset(self, timestamp_offset: int, profit_loss_offset: float) -> "ActivityLogRow":
        new_columns = self.columns[:]
        new_columns[1] += timestamp_offset
        new_columns[-1] += profit_loss_offset

        return ActivityLogRow(new_columns)

    def __str__(self) -> str:
        return ";".join(map(str, self.columns))

@dataclass
class TradeRow:
    trade: Trade

    @property
    def timestamp(self) -> int:
        return self.trade.timestamp

    def with_offset(self, timestamp_offset: int) -> "TradeRow":
        return TradeRow(Trade(
            self.trade.symbol,
            self.trade.price,
            self.trade.quantity,
            self.trade.buyer,
            self.trade.seller,
            self.trade.timestamp + timestamp_offset,
        ))

    def __str__(self) -> str:
        return "  " + f"""
  {{
    "timestamp": {self.trade.timestamp},
    "buyer": "{self.trade.buyer}",
    "seller": "{self.trade.seller}",
    "symbol": "{self.trade.symbol}",
    "currency": "SEASHELLS",
    "price": {self.trade.price},
    "quantity": {self.trade.quantity},
  }}
        """.strip()

@dataclass
class BacktestResult:
    round_num: int
    day_num: int

    sandbox_logs: list[SandboxLogRow]
    activity_logs: list[ActivityLogRow]
    trades: list[TradeRow]

@dataclass
class MarketTrade:
    trade: Trade
    buy_quantity: int
    sell_quantity: int
