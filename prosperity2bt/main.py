import json
import sys
import webbrowser
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from functools import partial, reduce
from http.server import HTTPServer, SimpleHTTPRequestHandler
from importlib import import_module, metadata
from pathlib import Path
from prosperity2bt.core import DayResult, run_backtest
from prosperity2bt.data import DayData, read_day_data, read_round_data
from prosperity2bt.datamodel import ProsperityEncoder
from typing import Any, Optional

def parse_algorithm(algorithm: str) -> tuple[Any, Any]:
    algorithm_path = Path(algorithm).expanduser().resolve()

    sys.path.append(str(algorithm_path.parent))
    return import_module(algorithm_path.stem).Trader

def parse_days(days: list[str], data_root: Optional[str]) -> list[DayData]:
    parsed_days = []

    if data_root is not None:
        data_root = Path(data_root).expanduser().resolve()

    for arg in days:
        if "-" in arg:
            round, day = map(int, arg.split("-", 1))
            parsed_days.append(read_day_data(data_root, round, day))
        else:
            round = int(arg)
            parsed_days.extend(read_round_data(data_root, round))

    return parsed_days

def parse_out(out: Optional[str]) -> Path:
    if out is not None:
        return Path(out).expanduser().resolve()
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return Path.cwd() / "backtests" / f"{timestamp}.log"

def offset_sandbox_log_row(row: Any, timestamp_offset: int) -> Any:
    old_timestamp = row["timestamp"]
    new_timestamp = old_timestamp + timestamp_offset

    row["lambdaLog"] = row["lambdaLog"].replace(f"[[{old_timestamp},", f"[[{new_timestamp},")
    row["timestamp"] += timestamp_offset

    return row

def offset_trade(trade: Any, timestamp_offset: int) -> Any:
    trade["timestamp"] += timestamp_offset
    return trade

def merge_results(a: DayResult, b: DayResult, merge_profit_loss: bool) -> DayResult:
    sandbox_logs = a.sandbox_logs[:]
    activity_logs = a.activity_logs[:]
    trades = a.trades[:]

    a_last_timestamp = a.activity_logs[-1].timestamp
    timestamp_offset = a_last_timestamp + 100

    sandbox_logs.extend([offset_sandbox_log_row(row, timestamp_offset) for row in b.sandbox_logs])

    if merge_profit_loss:
        profit_loss_offsets = defaultdict(float)
        for row in reversed(a.activity_logs):
            if row.timestamp != a_last_timestamp:
                break

            profit_loss_offsets[row.columns[2]] = row.columns[-1]

        activity_logs.extend([
            row.with_offset(timestamp_offset, profit_loss_offsets[row.columns[2]])
            for row in b.activity_logs
        ])
    else:
        activity_logs.extend([row.with_offset(timestamp_offset, 0) for row in b.activity_logs])

    trades.extend([offset_trade(trade, timestamp_offset) for trade in b.trades])

    return DayResult(a.round, a.day, sandbox_logs, activity_logs, trades)

def write_output(output_file: Path, merged_results: DayResult) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w+", encoding="utf-8") as file:
        file.write("Sandbox logs:\n")
        file.write("\n".join(json.dumps(row, indent=2) for row in merged_results.sandbox_logs))

        file.write("\n\n\n\nActivities log:\n")
        file.write("day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;mid_price;profit_and_loss\n")
        file.write("\n".join(map(str, merged_results.activity_logs)))

        file.write("\n\n\n\n\nTrade History:\n")
        file.write(json.dumps(merged_results.trades, cls=ProsperityEncoder, indent=2))

def print_overall_summary(results: list[DayResult]) -> None:
    print(f"Profit summary:")

    total_profit = 0
    for result in results:
        last_timestamp = result.activity_logs[-1].timestamp

        profit = 0
        for row in reversed(result.activity_logs):
            if row.timestamp != last_timestamp:
                break

            profit += row.columns[-1]

        print(f"Round {result.round} day {result.day}: {profit:,.0f}")
        total_profit += profit

    print(f"Total profit: {total_profit:,.0f}\n")

class HTTPRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        return super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return

def open_visualizer(output_file: Path) -> None:
    http_handler = partial(HTTPRequestHandler, directory=output_file.parent)
    http_server = HTTPServer(("localhost", 0), http_handler)

    webbrowser.open(f"https://jmerle.github.io/imc-prosperity-2-visualizer/?open=http://localhost:{http_server.server_port}/{output_file.name}")
    http_server.handle_request()
    http_server.handle_request()

def format_path(path: Path) -> str:
    cwd = Path.cwd()
    if path.is_relative_to(cwd):
        return str(path.relative_to(cwd))
    else:
        return str(path)

def main() -> None:
    parser = ArgumentParser(prog="prosperity2bt", description="Run a backtest.")
    parser.add_argument("algorithm", type=str, help="path to the Python file containing the algoritm to backtest")
    parser.add_argument("days", type=str, nargs="+", help="the days to backtest on (<round>-<day> for a single day, <round> for all days in a round)")
    parser.add_argument("--merge-pnl", action="store_true", help="merge profit and loss across days")
    parser.add_argument("--vis", action="store_true", help="open backtest result in visualizer when done")
    parser.add_argument("--out", type=str, help="path to save output log to (defaults to backtests/<timestamp>.log)")
    parser.add_argument("--data", type=str, help="path to data directory (must look similar in structure to https://github.com/jmerle/imc-prosperity-2-backtester/tree/master/prosperity2bt/resources)")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {metadata.version(__package__)}")

    args = parser.parse_args()

    Trader = parse_algorithm(args.algorithm)
    days = parse_days(args.days, args.data)
    output_file = parse_out(args.out)

    results = [run_backtest(Trader(), day) for day in days]
    merged_results = reduce(lambda a, b: merge_results(a, b, args.merge_pnl), results)

    write_output(output_file, merged_results)

    if len(days) > 1:
        print_overall_summary(results)

    print(f"Successfully saved backtest results to {format_path(output_file)}")

    if args.vis:
        open_visualizer(output_file)
