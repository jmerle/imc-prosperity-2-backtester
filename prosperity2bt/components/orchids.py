from prosperity2bt.datamodel import Order, Symbol, TradingState
from prosperity2bt.file_reader import FileReader
from prosperity2bt.models import Component, DayResults, SandboxLogRow
from typing import Optional

class OrchidsComponent(Component):
    """A Component implementation for ORCHIDS."""

    @staticmethod
    def create(file_reader: FileReader, round_num: int, day_num: int) -> Optional["Component"]:
        return None

    def pre_run(self, state: TradingState) -> None:
        pass

    def post_run(
            self,
            state: TradingState,
            orders: dict[Symbol, list[Order]],
            conversions: Optional[int],
            results: DayResults,
            sandbox_row: SandboxLogRow,
        ) -> None:
        pass
