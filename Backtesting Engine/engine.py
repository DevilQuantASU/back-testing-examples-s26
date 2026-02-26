import csv
import logging
import datetime
from collections import deque
from typing import List, Dict, Optional, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Event Classes ---
class Event:
    pass

class MarketEvent(Event):
    def __init__(self, data: Dict[str, Any]):
        self.data = data

class OrderEvent(Event):
    def __init__(self, event_id: str, timestamp: str, side: str, size: int):
        self.event_id = event_id
        self.timestamp = timestamp
        self.side = side  # "LONG" or "SHORT"
        self.size = size

class FillEvent(Event):
    def __init__(self, event_id: str, timestamp: str, side: str, size: int, price: float, fees: float):
        self.event_id = event_id
        self.timestamp = timestamp
        self.side = side
        self.size = size
        self.price = price
        self.fees = fees

# --- Output Objects ---
class Returns:
    def __init__(self, date: datetime.date, returns: float):
        self.date = date
        self.returns = returns

    def to_dict(self):
        return {
            "date": self.date.strftime("%Y-%m-%d"),
            "returns": self.returns
        }

class TradeLedger:
    def __init__(self, trade_id: str, event_id: str, entry_timestamp: str, entry_price: float,
                 exit_timestamp: str, exit_price: float, size: int, side: str, 
                 gross_pnl: float, fees: float, net_pnl: float, duration: float):
        self.trade_id = trade_id
        self.event_id = event_id
        self.entry_timestamp = entry_timestamp
        self.entry_price = entry_price
        self.exit_timestamp = exit_timestamp
        self.exit_price = exit_price
        self.size = size
        self.side = side
        self.gross_pnl = gross_pnl
        self.fees = fees
        self.net_pnl = net_pnl
        self.duration = duration

    def to_dict(self):
        return {
            "trade_id": self.trade_id,
            "event_id": self.event_id,
            "entry_timestamp": self.entry_timestamp,
            "entry_price": self.entry_price,
            "exit_timestamp": self.exit_timestamp,
            "exit_price": self.exit_price,
            "size": self.size,
            "side": self.side,
            "gross_pnl": self.gross_pnl,
            "fees": self.fees,
            "net_pnl": self.net_pnl,
            "duration": self.duration
        }

# --- Core Engine Components ---
class DataHandler:
    def __init__(self, csv_file: str, events_queue: deque):
        self.csv_file = csv_file
        self.events_queue = events_queue
        self.data_generator = self._read_csv()
        self.current_data = None
        self.continue_backtest = True

    def _read_csv(self):
        with open(self.csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Type conversions
                row['last_price'] = float(row['last_price']) if row['last_price'] else 0.0
                row['best_bid'] = float(row['best_bid']) if row['best_bid'] else None
                row['best_ask'] = float(row['best_ask']) if row['best_ask'] else None
                row['bid_size'] = int(row['bid_size']) if row['bid_size'] else 0
                row['ask_size'] = int(row['ask_size']) if row['ask_size'] else 0
                row['volume'] = int(row['volume']) if row['volume'] else 0
                row['is_settled'] = int(row['is_settled'])
                yield row

    def update_bars(self):
        try:
            self.current_data = next(self.data_generator)
            self.events_queue.append(MarketEvent(self.current_data))
        except StopIteration:
            self.continue_backtest = False

    def get_latest_data(self) -> Optional[Dict[str, Any]]:
        return self.current_data

class ExecutionHandler:
    """
    Simulates execution of orders.
    Extensible for slippage and fee modeling.
    """
    def __init__(self, events_queue: deque, data_handler: DataHandler):
        self.events_queue = events_queue
        self.data_handler = data_handler

    def execute_order(self, event: OrderEvent):
        current_data = self.data_handler.get_latest_data()
        if not current_data:
            return

        # Simple execution model: fill at last_price without slippage and zero fees for now.
        # Extensible: Check bid/ask spread, volume constraints, add slippage function here.
        price = current_data['last_price']
        if event.side == "LONG" and current_data['best_ask'] is not None:
             price = current_data['best_ask']
        elif event.side == "SHORT" and current_data['best_bid'] is not None:
             price = current_data['best_bid']

        fees = 0.0 # Placeholder for fee logic

        fill_event = FillEvent(
            event_id=event.event_id,
            timestamp=current_data['timestamp'],
            side=event.side,
            size=event.size,
            price=price,
            fees=fees
        )
        self.events_queue.append(fill_event)

class Position:
    def __init__(self, event_id: str, side: str, entry_timestamp: str, entry_price: float, size: int):
        self.event_id = event_id
        self.side = side # "LONG" or "SHORT". In binary options, usually we only buy YES (Long) or buy NO (Short)
        self.entry_timestamp = entry_timestamp
        self.entry_price = entry_price
        self.size = size

class Portfolio:
    def __init__(self, events_queue: deque, initial_capital: float = 10000.0):
        self.events_queue = events_queue
        # Dictionary of active positions: event_id -> list of Position objects (for simplicity)
        self.positions: Dict[str, List[Position]] = {}
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        
        self.trade_ledgers: List[TradeLedger] = []
        self.equity_curve: List[Dict[str, Any]] = [] # Tracks daily equity
        
        self.last_date = None
        self.last_equity = initial_capital
        self.trade_counter = 0

    def update_timeindex(self, event: MarketEvent):
        # Called on new market data to update mark-to-market equity and calculate returns
        timestamp_str = event.data['timestamp']
        dt = datetime.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        current_date = dt.date()
        
        # Calculate current equity
        total_equity = self.current_cash
        last_price = event.data['last_price']
        
        for event_id, pos_list in self.positions.items():
            for p in pos_list:
                if p.side == "LONG":
                    # Value of holding YES: size * current_price
                    total_equity += p.size * last_price
                elif p.side == "SHORT":
                    # Value of holding NO: size * (1 - current_price)  (assuming prices 0 to 1)
                    # If prices are 0 to 100, we'd use 100 - p
                    max_p = 100.0 if last_price > 1.0 else 1.0
                    total_equity += p.size * (max_p - last_price)

        if self.last_date is None:
            self.last_date = current_date
            self.last_equity = total_equity
        elif current_date > self.last_date:
            # We hit a new day, record returns for the previous day
            daily_return = (total_equity - self.last_equity) / self.last_equity if self.last_equity > 0 else 0
            ret_obj = Returns(self.last_date, daily_return)
            self.equity_curve.append(ret_obj)
            
            self.last_date = current_date
            self.last_equity = total_equity

    def update_fill(self, event: FillEvent):
        # Simplistic portfolio management: 
        # For simplicity, if we get a LONG fill and have a SHORT position, we close it (reduce size) - and vice versa.
        # Otherwise we open a new position.
        
        event_positions = self.positions.get(event.event_id, [])
        remaining_size = event.size

        new_positions = []
        for p in event_positions:
            if remaining_size <= 0:
                new_positions.append(p)
                continue

            if p.side != event.side:
                # Closing out an opposite position
                close_size = min(p.size, remaining_size)
                p.size -= close_size
                remaining_size -= close_size
                
                # Deduct value and calc PnL
                entry_timestamp_dt = datetime.datetime.strptime(p.entry_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                exit_timestamp_dt = datetime.datetime.strptime(event.timestamp, "%Y-%m-%dT%H:%M:%SZ")
                duration_hours = (exit_timestamp_dt - entry_timestamp_dt).total_seconds() / 3600.0

                if p.side == "LONG":
                    # Bought at entry_price, selling here
                    gross_pnl = close_size * (event.price - p.entry_price)
                    self.current_cash += close_size * event.price
                else: # p.side == "SHORT"
                    # Shorted at entry_price, buying back here
                    # Actually Polymarket shorting means buying NO. 
                    gross_pnl = close_size * (p.entry_price - event.price)
                    self.current_cash += close_size * (1.0 - event.price) # Adjust if 100 scale

                self.current_cash -= event.fees
                net_pnl = gross_pnl - event.fees

                self.trade_counter += 1
                tl = TradeLedger(
                    trade_id=f"TRD_{self.trade_counter}",
                    event_id=event.event_id,
                    entry_timestamp=p.entry_timestamp,
                    entry_price=p.entry_price,
                    exit_timestamp=event.timestamp,
                    exit_price=event.price,
                    size=close_size,
                    side=p.side,
                    gross_pnl=gross_pnl,
                    fees=event.fees,
                    net_pnl=net_pnl,
                    duration=duration_hours
                )
                self.trade_ledgers.append(tl)

                if p.size > 0:
                    new_positions.append(p)
            else:
                new_positions.append(p)

        if remaining_size > 0:
            # Open new position
            cost = remaining_size * event.price
            self.current_cash -= cost
            self.current_cash -= event.fees
            new_pos = Position(event.event_id, event.side, event.timestamp, event.price, remaining_size)
            new_positions.append(new_pos)

        self.positions[event.event_id] = new_positions

    def get_trade_ledgers(self) -> List[Dict]:
        return [t.to_dict() for t in self.trade_ledgers]

    def get_returns(self) -> List[Dict]:
        return [r.to_dict() for r in self.equity_curve]

class BaseStrategy:
    def __init__(self, data_handler: DataHandler, events_queue: deque):
        self.data_handler = data_handler
        self.events_queue = events_queue

    def calculate_signals(self, event: MarketEvent):
        raise NotImplementedError("Strategy must implement calculate_signals")

    def buy(self, event_id: str, timestamp: str, size: int):
        self.events_queue.append(OrderEvent(event_id, timestamp, "LONG", size))
        
    def sell(self, event_id: str, timestamp: str, size: int):
        self.events_queue.append(OrderEvent(event_id, timestamp, "SHORT", size))

class BacktestEngine:
    def __init__(self, csv_file: str, strategy_class: type, initial_capital: float = 10000.0):
        self.events_queue = deque()
        self.data_handler = DataHandler(csv_file, self.events_queue)
        self.portfolio = Portfolio(self.events_queue, initial_capital)
        self.execution_handler = ExecutionHandler(self.events_queue, self.data_handler)
        self.strategy = strategy_class(self.data_handler, self.events_queue)

    def run(self):
        logger.info("Starting Backtest...")
        
        while True:
            # Fetch new data bars
            if self.data_handler.continue_backtest:
                self.data_handler.update_bars()
            else:
                break

            # Handle the events queue synchronously per time step
            while self.events_queue:
                event = self.events_queue.popleft()
                
                if isinstance(event, MarketEvent):
                    self.portfolio.update_timeindex(event)
                    self.strategy.calculate_signals(event)
                elif isinstance(event, OrderEvent):
                    self.execution_handler.execute_order(event)
                elif isinstance(event, FillEvent):
                    self.portfolio.update_fill(event)
                    
        logger.info("Backtest Complete. Generating Output Files...")

    def output_results(self, trades_file: str = "trade_ledger.csv", returns_file: str = "returns.csv"):
        trades = self.portfolio.get_trade_ledgers()
        if trades:
            with open(trades_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(trades[0].keys()))
                writer.writeheader()
                writer.writerows(trades)
            logger.info(f"Wrote trades to {trades_file}")
        else:
            logger.info("No trades executed.")

        returns = self.portfolio.get_returns()
        if returns:
            with open(returns_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(returns[0].keys()))
                writer.writeheader()
                writer.writerows(returns)
            logger.info(f"Wrote returns to {returns_file}")
        else:
            logger.info("No return data generated.")
