import sys
import webbrowser
from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from functools import partial, reduce
from http.server import HTTPServer, SimpleHTTPRequestHandler
from importlib import import_module, metadata, reload
from pathlib import Path
from prosperity2bt.components.basic_products import BasicProductsComponent
from prosperity2bt.components.orchids import OrchidsComponent
from prosperity2bt.file_reader import FileSystemReader, PackageResourcesReader
from prosperity2bt.models import DayResults
from prosperity2bt.runner import DayConfig, run_backtest
from typing import Any, Optional

def parse_algorithm(algorithm: str) -> Any:
    algorithm_path = Path(algorithm).expanduser().resolve()

    sys.path.append(str(algorithm_path.parent))
    return import_module(algorithm_path.stem)

def parse_days(args: Any) -> list[DayConfig]:
    if args.data is not None:
        file_reader = FileSystemReader(Path(args.data).expanduser().resolve())
    else:
        file_reader = PackageResourcesReader()

    parsed_days = []
    component_classes = [
        BasicProductsComponent,
        OrchidsComponent,
    ]

    for arg in args.days:
        if "-" in arg:
            round_num, day_num = map(int, arg.split("-", 1))

            components = [clazz.create(file_reader, round_num, day_num) for clazz in component_classes]
            components = [c for c in components if c is not None]

            if len(components) == 0:
                print(f"Warning: no data exists for round {round_num} day {day_num}")
                continue

            parsed_days.append(DayConfig(round_num, day_num, components, args))
        else:
            round_num = int(arg)

            parsed_days_in_round = []
            for day_num in range(-5, 6):
                components = [clazz.create(file_reader, round_num, day_num) for clazz in component_classes]
                components = [c for c in components if c is not None]

                if len(components) > 0:
                    parsed_days_in_round.append(DayConfig(round_num, day_num, components, args))

            if len(parsed_days_in_round) == 0:
                print(f"Warning: no data found for round {round_num}")
                continue

            parsed_days.extend(parsed_days_in_round)

    return parsed_days

def parse_out(out: Optional[str]) -> Path:
    if out is not None:
        return Path(out).expanduser().resolve()
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return Path.cwd() / "backtests" / f"{timestamp}.log"

def print_day_summary(results: DayResults) -> None:
    last_timestamp = results.activity_logs[-1].timestamp

    product_lines = []
    total_profit = 0

    for row in reversed(results.activity_logs):
        if row.timestamp != last_timestamp:
            break

        product = row.columns[2]
        profit = row.columns[-1]

        product_lines.append(f"{product}: {profit:,.0f}")
        total_profit += profit

    print(*reversed(product_lines), sep="\n")
    print(f"Total profit: {total_profit:,.0f}\n")

def merge_results(a: DayResults, b: DayResults, merge_profit_loss: bool) -> DayResults:
    sandbox_logs = a.sandbox_logs[:]
    activity_logs = a.activity_logs[:]
    trades = a.trades[:]

    a_last_timestamp = a.activity_logs[-1].timestamp
    timestamp_offset = a_last_timestamp + 100

    sandbox_logs.extend([row.with_offset(timestamp_offset) for row in b.sandbox_logs])

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

    trades.extend([row.with_offset(timestamp_offset) for row in b.trades])
    return DayResults(a.round_num, a.day_num, sandbox_logs, activity_logs, trades)

def write_output(output_file: Path, merged_results: DayResults) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w+", encoding="utf-8") as file:
        file.write("Sandbox logs:\n")
        file.write("\n".join(map(str, merged_results.sandbox_logs)))

        file.write("\n\n\n\nActivities log:\n")
        file.write("day;timestamp;product;bid_price_1;bid_volume_1;bid_price_2;bid_volume_2;bid_price_3;bid_volume_3;ask_price_1;ask_volume_1;ask_price_2;ask_volume_2;ask_price_3;ask_volume_3;mid_price;profit_and_loss\n")
        file.write("\n".join(map(str, merged_results.activity_logs)))

        file.write("\n\n\n\n\nTrade History:\n")
        file.write("[\n")
        file.write(",\n".join(map(str, merged_results.trades)))
        file.write("]")

def print_overall_summary(results: list[DayResults]) -> None:
    print(f"Profit summary:")

    total_profit = 0
    for result in results:
        last_timestamp = result.activity_logs[-1].timestamp

        profit = 0
        for row in reversed(result.activity_logs):
            if row.timestamp != last_timestamp:
                break

            profit += row.columns[-1]

        print(f"Round {result.round_num} day {result.day_num}: {profit:,.0f}")
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
    parser.add_argument("--print", action="store_true", help="print the trader's output to stdout while it's running")
    parser.add_argument("--no-trades-matching", action="store_true", help="disable matching orders against market trades")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {metadata.version(__package__)}")

    args = parser.parse_args()

    trader_module = parse_algorithm(args.algorithm)
    days = parse_days(args)
    output_file = parse_out(args.out)

    results = []
    for day in days:
        print(f"Backtesting {args.algorithm} on round {day.round_num} day {day.day_num}")

        reload(trader_module)
        trader = trader_module.Trader()

        result = run_backtest(trader, day)
        print_day_summary(result)

        results.append(result)

    merged_results = reduce(lambda a, b: merge_results(a, b, args.merge_pnl), results)

    write_output(output_file, merged_results)

    if len(days) > 1:
        print_overall_summary(results)

    print(f"Successfully saved backtest results to {format_path(output_file)}")

    if args.vis:
        open_visualizer(output_file)

if __name__ == "__main__":
    main()
