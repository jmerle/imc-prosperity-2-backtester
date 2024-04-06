from collections import defaultdict
from contextlib import redirect_stdout
from dataclasses import dataclass
from io import StringIO
from prosperity2bt.data import DayData, LIMITS, PriceRow
from prosperity2bt.datamodel import Observation, Order, OrderDepth, Symbol, Trade, TradingState
from typing import Any

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
class DayResult:
    round: int
    day: int

    sandbox_logs: list[dict[str, Any]]
    activity_logs: list[ActivityLogRow]
    trades: list[Trade]

def check_limits(
    tradable_products: list[str],
    orders_by_symbol: dict[Symbol, list[Order]],
    own_positions: dict[str, int],
) -> None:
    sandbox_log_lines = []
    for product in tradable_products:
        orders = orders_by_symbol.get(product, [])

        current_position = own_positions[product]

        total_long = sum(order.quantity for order in orders if order.quantity > 0)
        total_short = sum(abs(order.quantity) for order in orders if order.quantity < 0)

        if current_position + total_long > LIMITS[product] or current_position - total_short < -LIMITS[product]:
            sandbox_log_lines.append(f"Orders for product {product} exceeded limit of {LIMITS[product]} set")
            orders_by_symbol.pop(product)

    if len(sandbox_log_lines) == 0:
        return ""

    return "\n" + "\n".join(sandbox_log_lines)

def process_buy_order(
    timestamp: int,
    order: Order,
    order_depth: OrderDepth,
    own_trades: dict[Symbol, list[Trade]],
    own_positions: dict[Symbol, int],
    profit_loss_by_product: dict[Symbol, float],
) -> list[Trade]:
    new_trades = []

    price_matches = sorted(price for price in order_depth.sell_orders.keys() if price <= order.price)
    for price in price_matches:
        volume = min(order.quantity, abs(order_depth.sell_orders[price]))

        new_trades.append(Trade(order.symbol, price, volume, "SUBMISSION", "", timestamp))

        own_positions[order.symbol] += volume
        profit_loss_by_product[order.symbol] -= price * volume

        order_depth.sell_orders[price] += volume
        if order_depth.sell_orders[price] == 0:
            order_depth.sell_orders.pop(price)

        order.quantity -= volume
        if order.quantity == 0:
            break

    return new_trades

def process_sell_order(
    timestamp: int,
    order: Order,
    order_depth: OrderDepth,
    own_trades: dict[Symbol, list[Trade]],
    own_positions: dict[Symbol, int],
    profit_loss_by_product: dict[Symbol, float],
) -> list[Trade]:
    new_trades = []

    price_matches = sorted((price for price in order_depth.buy_orders.keys() if price >= order.price), reverse=True)
    for price in price_matches:
        volume = min(abs(order.quantity), order_depth.buy_orders[price])

        new_trades.append(Trade(order.symbol, price, volume, "", "SUBMISSION", timestamp))

        own_positions[order.symbol] -= volume
        profit_loss_by_product[order.symbol] += price * volume

        order_depth.buy_orders[price] -= volume
        if order_depth.buy_orders[price] == 0:
            order_depth.buy_orders.pop(price)

        order.quantity += volume
        if order.quantity == 0:
            break

    return new_trades

def process_order(
    timestamp: int,
    order: Order,
    order_depths: dict[Symbol, OrderDepth],
    own_trades: dict[Symbol, list[Trade]],
    own_positions: dict[Symbol, int],
    profit_loss_by_product: dict[Symbol, float],
) -> list[Trade]:
    order_depth = order_depths[order.symbol]
    if order.quantity > 0:
        return process_buy_order(timestamp, order, order_depth, own_trades, own_positions, profit_loss_by_product)
    elif order.quantity < 0:
        return process_sell_order(timestamp, order, order_depth, own_trades, own_positions, profit_loss_by_product)
    else:
        return []

def trade_to_dict(trade: Trade) -> dict[str, Any]:
    return {
        "timestamp": trade.timestamp,
        "buyer": trade.buyer,
        "seller": trade.seller,
        "symbol": trade.symbol,
        "currency": "SEASHELLS",
        "price": trade.price,
        "quantity": trade.quantity,
    }

def create_activity_log_row(
    day: int,
    timestamp: int,
    product: str,
    price: PriceRow,
    profit_loss: float,
) -> ActivityLogRow:
    bid_prices_len = len(price.bid_prices)
    bid_volumes_len = len(price.bid_volumes)
    ask_prices_len = len(price.ask_prices)
    ask_volumes_len = len(price.ask_volumes)

    columns = [
        day,
        timestamp,
        product,
        price.bid_prices[0] if bid_prices_len > 0 else "",
        price.bid_volumes[0] if bid_volumes_len > 0 else "",
        price.bid_prices[1] if bid_prices_len > 1 else "",
        price.bid_volumes[1] if bid_volumes_len > 1 else "",
        price.bid_prices[2] if bid_prices_len > 2 else "",
        price.bid_volumes[2] if bid_volumes_len > 2 else "",
        price.ask_prices[0] if ask_prices_len > 0 else "",
        price.ask_volumes[0] if ask_volumes_len > 0 else "",
        price.ask_prices[1] if ask_prices_len > 1 else "",
        price.ask_volumes[1] if ask_volumes_len > 1 else "",
        price.ask_prices[2] if ask_prices_len > 2 else "",
        price.ask_volumes[2] if ask_volumes_len > 2 else "",
        price.mid_price,
        profit_loss,
    ]

    return ActivityLogRow(columns)

def print_backtest_summary(result: DayResult, tradable_products: list[str]) -> None:
    last_timestamp = result.activity_logs[-1].timestamp

    for product in sorted(tradable_products):
        product_profit = next(
            row.columns[-1] for row in reversed(result.activity_logs)
            if row.timestamp == last_timestamp and row.columns[2] == product
        )

        print(f"{product}: {product_profit:,.0f}")

    total_profit = 0
    for row in reversed(result.activity_logs):
        if row.timestamp != last_timestamp:
            break

        total_profit += row.columns[-1]

    print(f"Total profit: {total_profit:,.0f}\n")

def run_backtest(trader: Any, data: DayData) -> DayResult:
    print(f"Backtesting {trader.__module__} on round {data.round} day {data.day}")

    result = DayResult(data.round, data.day, [], [], [])
    trader_data = ""

    prices_by_timestamp = defaultdict(dict)
    for row in data.prices:
        prices_by_timestamp[row.timestamp][row.product] = row

    trades_by_timestamp = defaultdict(lambda: defaultdict(list))
    for trade in data.trades:
        trades_by_timestamp[trade.timestamp][trade.symbol].append(trade)

    tradable_products = sorted(set(row.product for row in data.prices))

    listings = {product: {
        "symbol": product,
        "product": product,
        "denomination": 1,
    } for product in tradable_products}

    own_positions = defaultdict(int)
    own_trades = defaultdict(list)
    market_trades = defaultdict(list)

    profit_loss_by_product = defaultdict(float)

    for timestamp in sorted(prices_by_timestamp.keys()):
        order_depths = {}
        for product in tradable_products:
            row = prices_by_timestamp[timestamp][product]
            order_depths[product] = OrderDepth()

            for price, volume in zip(row.bid_prices, row.bid_volumes):
                order_depths[product].buy_orders[price] = volume

            for price, volume in zip(row.ask_prices, row.ask_volumes):
                order_depths[product].sell_orders[price] = -volume

        position = {product: position for product, position in own_positions.items() if position != 0}
        observations = Observation({}, {})

        state = TradingState(
            trader_data,
            timestamp,
            listings,
            order_depths,
            dict(own_trades),
            dict(market_trades),
            position,
            observations,
        )

        stdout = StringIO()
        with redirect_stdout(stdout):
            orders_by_symbol, conversions, trader_data = trader.run(state)

        for product in tradable_products:
            price = prices_by_timestamp[timestamp][product]

            profit_loss = profit_loss_by_product[product]
            if own_positions[product] != 0:
                profit_loss += own_positions[product] * price.mid_price

            result.activity_logs.append(create_activity_log_row(data.day, timestamp, product, price, profit_loss))

        sandbox_log = check_limits(tradable_products, orders_by_symbol, own_positions)

        result.sandbox_logs.append({
            "sandboxLog": sandbox_log,
            "lambdaLog": stdout.getvalue().rstrip(),
            "timestamp": timestamp,
        })

        for product in tradable_products:
            new_trades = []

            for order in orders_by_symbol.get(product, []):
                new_trades.extend(process_order(
                    timestamp,
                    order,
                    order_depths,
                    own_trades,
                    own_positions,
                    profit_loss_by_product,
                ))

            if len(new_trades) > 0:
                own_trades[product] = new_trades

        for product, trades in trades_by_timestamp[timestamp].items():
            market_trades[product] = trades

        current_trades = []
        for product in tradable_products:
            current_trades.extend([trade_to_dict(trade) for trade in own_trades[product]])
        for product in tradable_products:
            current_trades.extend([trade_to_dict(trade) for trade in trades_by_timestamp[timestamp][product]])

        result.trades.extend(current_trades)

    print_backtest_summary(result, tradable_products)
    return result
