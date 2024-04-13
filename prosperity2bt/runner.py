from dataclasses import dataclass
from contextlib import closing, redirect_stdout
from io import StringIO
from IPython.utils.io import Tee
from prosperity2bt.models import Component, DayResults, SandboxLogRow
from prosperity2bt.datamodel import Observation, TradingState
from typing import Any

@dataclass
class DayConfig:
    round_num: int
    day_num: int
    components: list[Component]
    args: Any

def run_backtest(trader: Any, config: DayConfig) -> DayResults:
    # We assume all components support the same timestamps for a given day
    timestamps = sorted(config.components[0].supported_timestamps())

    trader_data = ""
    state = TradingState(
        traderData=trader_data,
        timestamp=0,
        listings={},
        order_depths={},
        own_trades={},
        market_trades={},
        position={},
        observations=Observation({}, {}),
    )

    results = DayResults(
        round_num=config.round_num,
        day_num=config.day_num,
        sandbox_logs=[],
        activity_logs=[],
        trades=[],
    )

    for timestamp in timestamps:
        state.timestamp = timestamp
        state.traderData = trader_data

        for component in config.components:
            component.pre_run(config.args, state)

        stdout = StringIO()

        # Tee calls stdout.close(), making stdout.getvalue() impossible
        # This override makes getvalue() possible after close()
        stdout.close = lambda: None

        if config.args.print:
            with closing(Tee(stdout)):
                orders, conversions, trader_data = trader.run(state)
        else:
            with redirect_stdout(stdout):
                orders, conversions, trader_data = trader.run(state)

        sandbox_row = SandboxLogRow(
            timestamp=timestamp,
            sandbox_log="",
            lambda_log=stdout.getvalue().rstrip(),
        )

        results.sandbox_logs.append(sandbox_row)

        for component in config.components:
            component.post_run(config.args, state, orders, conversions, results, sandbox_row)

    return results
