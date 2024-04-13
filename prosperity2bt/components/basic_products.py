from collections import defaultdict
from dataclasses import dataclass
from prosperity2bt.constants import LIMITS
from prosperity2bt.datamodel import Order, OrderDepth, Symbol, Trade, TradingState
from prosperity2bt.file_reader import FileReader
from prosperity2bt.models import ActivityLogRow, Component, DayResults, SandboxLogRow, TradeRow
from typing import Any, Optional

@dataclass
class PriceRow:
    day: int
    timestamp: int
    product: Symbol
    bid_prices: list[int]
    bid_volumes: list[int]
    ask_prices: list[int]
    ask_volumes: list[int]
    mid_price: float
    profit_loss: float

def get_column_values(columns: list[str], indices: list[int]) -> list[int]:
    values = []

    for index in indices:
        value = columns[index]
        if value == "":
            break

        values.append(int(value))

    return values

@dataclass
class MarketTrade:
    trade: Trade
    buy_quantity: int
    sell_quantity: int

class BasicProductsComponent(Component):
    """A Component implementation for "basic" products like AMETHYSTS and STARFRUIT."""

    def __init__(self, prices: list[PriceRow], trades: list[Trade]) -> None:
        self._products = sorted(set(row.product for row in prices))

        self._prices: dict[int, dict[Symbol, PriceRow]] = defaultdict(dict)
        for row in prices:
            self._prices[row.timestamp][row.product] = row

        self._trades: dict[int, dict[Symbol, list[Trade]]] = defaultdict(lambda: defaultdict(list))
        for trade in trades:
            self._trades[trade.timestamp][trade.symbol].append(trade)

        self._profit_loss: dict[Symbol, int] = defaultdict(int)

    @staticmethod
    def create(file_reader: FileReader, round_num: int, day_num: int) -> Optional["Component"]:
        prices = []
        with file_reader.file([f"round{round_num}", f"prices_round_{round_num}_day_{day_num}.csv"]) as file:
            if file is None:
                return None

            lines = file.read_text(encoding="utf-8").splitlines()
            if lines[0] != "day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;mid_price;profit_and_loss":
                return None

            for line in lines[1:]:
                columns = line.split(";")

                prices.append(PriceRow(
                    day=int(columns[0]),
                    timestamp=int(columns[1]),
                    product=columns[2],
                    bid_prices=get_column_values(columns, [3, 5, 7]),
                    bid_volumes=get_column_values(columns, [4, 6, 8]),
                    ask_prices=get_column_values(columns, [9, 11, 13]),
                    ask_volumes=get_column_values(columns, [10, 12, 14]),
                    mid_price=float(columns[15]),
                    profit_loss=float(columns[16]),
                ))

        trades = []
        for suffix in ["wn", "nn"]:
            with file_reader.file([f"round{round_num}", f"trades_round_{round_num}_day_{day_num}_{suffix}.csv"]) as file:
                if file is None:
                    continue

                lines = file.read_text(encoding="utf-8").splitlines()
                if lines[0] != "timestamp;buyer;seller;symbol;currency;price;quantity":
                    return None

                for line in lines[1:]:
                    columns = line.split(";")

                    trades.append(Trade(
                        symbol=columns[3],
                        price=int(float(columns[5])),
                        quantity=int(columns[6]),
                        buyer=columns[1],
                        seller=columns[2],
                        timestamp=int(columns[0]),
                    ))

                break

        return BasicProductsComponent(prices, trades)

    def supported_timestamps(self) -> list[int]:
        return list(self._prices.keys())

    def pre_run(self, args: Any, state: TradingState) -> None:
        for product in self._products:
            order_depth = OrderDepth()
            row = self._prices[state.timestamp][product]

            for price, volume in zip(row.bid_prices, row.bid_volumes):
                order_depth.buy_orders[price] = volume

            for price, volume in zip(row.ask_prices, row.ask_volumes):
                order_depth.sell_orders[price] = -volume

            state.order_depths[product] = order_depth

            state.listings[product] = {
                "symbol": product,
                "product": product,
                "denomination": 1,
            }

    def post_run(
            self,
            args: Any,
            state: TradingState,
            orders: dict[Symbol, list[Order]],
            conversions: Optional[int],
            results: DayResults,
            sandbox_row: SandboxLogRow,
        ) -> None:
        self._create_activity_logs(state, results)
        self._enforce_limits(state, orders, sandbox_row)
        self._match_orders(args, state, orders, results)

    def _create_activity_logs(self, state: TradingState, results: DayResults) -> None:
        for product in self._products:
            row = self._prices[state.timestamp][product]

            profit_loss = self._profit_loss[product]

            position = state.position.get(product, 0)
            if position != 0:
                profit_loss += position * row.mid_price

            bid_prices_len = len(row.bid_prices)
            bid_volumes_len = len(row.bid_volumes)
            ask_prices_len = len(row.ask_prices)
            ask_volumes_len = len(row.ask_volumes)

            columns = [
                results.day_num,
                state.timestamp,
                product,
                row.bid_prices[0] if bid_prices_len > 0 else "",
                row.bid_volumes[0] if bid_volumes_len > 0 else "",
                row.bid_prices[1] if bid_prices_len > 1 else "",
                row.bid_volumes[1] if bid_volumes_len > 1 else "",
                row.bid_prices[2] if bid_prices_len > 2 else "",
                row.bid_volumes[2] if bid_volumes_len > 2 else "",
                row.ask_prices[0] if ask_prices_len > 0 else "",
                row.ask_volumes[0] if ask_volumes_len > 0 else "",
                row.ask_prices[1] if ask_prices_len > 1 else "",
                row.ask_volumes[1] if ask_volumes_len > 1 else "",
                row.ask_prices[2] if ask_prices_len > 2 else "",
                row.ask_volumes[2] if ask_volumes_len > 2 else "",
                row.mid_price,
                profit_loss,
            ]

            results.activity_logs.append(ActivityLogRow(columns))

    def _enforce_limits(self, state: TradingState, orders: dict[Symbol, list[Order]], sandbox_row: SandboxLogRow) -> None:
        sandbox_log_lines = []
        for product in self._products:
            product_orders = orders.get(product, [])
            product_position = state.position.get(product, 0)

            total_long = sum(order.quantity for order in product_orders if order.quantity > 0)
            total_short = sum(abs(order.quantity) for order in product_orders if order.quantity < 0)

            if product_position + total_long > LIMITS[product] or product_position - total_short < -LIMITS[product]:
                sandbox_log_lines.append(f"Orders for product {product} exceeded limit of {LIMITS[product]} set")
                orders.pop(product)

        if len(sandbox_log_lines) > 0:
            sandbox_row.sandbox_log += "\n" + "\n".join(sandbox_log_lines)

    def _match_orders(self, args: Any, state: TradingState, orders: dict[Symbol, list[Order]], results: DayResults) -> None:
        market_trades: dict[Symbol, list[MarketTrade]] = {}
        for product, trades in self._trades[state.timestamp].items():
            market_trades[product] = [MarketTrade(t, t.quantity, t.quantity) for t in trades]

        for product in self._products:
            new_trades = []

            for order in orders.get(product, []):
                new_trades.extend(self._match_order(
                    state,
                    order,
                    [] if args.no_trades_matching else market_trades.get(product, []),
                ))

            if len(new_trades) > 0:
                state.own_trades[product] = new_trades
                results.trades.extend([TradeRow(trade) for trade in new_trades])

        for product, trades in market_trades.items():
            for trade in trades:
                trade.trade.quantity = min(trade.buy_quantity, trade.sell_quantity)

            remaining_market_trades = [t.trade for t in trades if t.trade.quantity > 0]

            state.market_trades[product] = remaining_market_trades
            results.trades.extend([TradeRow(trade) for trade in remaining_market_trades])

    def _match_order(self, state: TradingState, order: Order, market_trades: list[MarketTrade]) -> list[Trade]:
        if order.quantity > 0:
            return self._match_buy_order(state, order, market_trades)
        elif order.quantity < 0:
            return self._match_sell_order(state, order, market_trades)
        else:
            return []

    def _match_buy_order(self, state: TradingState, order: Order, market_trades: list[MarketTrade]) -> list[Trade]:
        trades = []

        order_depth = state.order_depths[order.symbol]
        price_matches = sorted(price for price in order_depth.sell_orders.keys() if price <= order.price)
        for price in price_matches:
            volume = min(order.quantity, abs(order_depth.sell_orders[price]))

            trades.append(Trade(order.symbol, price, volume, "SUBMISSION", "", state.timestamp))

            state.position[order.symbol] = state.position.get(order.symbol, 0) + volume
            self._profit_loss[order.symbol] -= price * volume

            order_depth.sell_orders[price] += volume
            if order_depth.sell_orders[price] == 0:
                order_depth.sell_orders.pop(price)

            order.quantity -= volume
            if order.quantity == 0:
                return trades

        for market_trade in market_trades:
            if market_trade.sell_quantity == 0 or market_trade.trade.price > order.price:
                continue

            volume = min(order.quantity, market_trade.sell_quantity)

            trades.append(Trade(order.symbol, order.price, volume, "SUBMISSION", market_trade.trade.seller, state.timestamp))

            state.position[order.symbol] = state.position.get(order.symbol, 0) + volume
            self._profit_loss[order.symbol] -= order.price * volume

            market_trade.sell_quantity -= volume

            order.quantity -= volume
            if order.quantity == 0:
                return trades

        return trades

    def _match_sell_order(self, state: TradingState, order: Order, market_trades: list[MarketTrade]) -> list[Trade]:
        trades = []

        order_depth = state.order_depths[order.symbol]
        price_matches = sorted((price for price in order_depth.buy_orders.keys() if price >= order.price), reverse=True)
        for price in price_matches:
            volume = min(abs(order.quantity), order_depth.buy_orders[price])

            trades.append(Trade(order.symbol, price, volume, "", "SUBMISSION", state.timestamp))

            state.position[order.symbol] = state.position.get(order.symbol, 0) - volume
            self._profit_loss[order.symbol] += price * volume

            order_depth.buy_orders[price] -= volume
            if order_depth.buy_orders[price] == 0:
                order_depth.buy_orders.pop(price)

            order.quantity += volume
            if order.quantity == 0:
                return trades

        for market_trade in market_trades:
            if market_trade.buy_quantity == 0 or market_trade.trade.price < order.price:
                continue

            volume = min(abs(order.quantity), market_trade.buy_quantity)

            trades.append(Trade(order.symbol, order.price, volume, market_trade.trade.buyer, "SUBMISSION", state.timestamp))

            state.position[order.symbol] = state.position.get(order.symbol, 0) - volume
            self._profit_loss[order.symbol] += order.price * volume

            market_trade.buy_quantity -= volume

            order.quantity += volume
            if order.quantity == 0:
                return trades

        return trades
