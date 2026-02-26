import logging
from engine import BaseStrategy, BacktestEngine, MarketEvent

logger = logging.getLogger(__name__)

class FrequentTradingStrategy(BaseStrategy):
    """
    A strategy that trades frequently (flips position every 100 ticks) 
    to generate significant trading volume regardless of signals.
    """
    def __init__(self, data_handler, events_queue):
        super().__init__(data_handler, events_queue)
        self.invested = False
        self.tick_count = 0
        self.flip_interval = 100

    def calculate_signals(self, event: MarketEvent):
        data = event.data
        price = data['last_price']
        event_id = data['event_id']
        timestamp = data['timestamp']

        self.tick_count += 1

        # Use larger size to see meaningful percent returns on 10,000 capital
        trade_size = 5000 

        if self.tick_count % self.flip_interval == 0:
            if not self.invested:
                # logger.info(f"Signal: BUY {trade_size} at {price} on {timestamp}")
                self.buy(event_id, timestamp, size=trade_size)
                self.invested = True
            else:
                # logger.info(f"Signal: SELL (close) {trade_size} at {price} on {timestamp}")
                self.sell(event_id, timestamp, size=trade_size)
                self.invested = False

if __name__ == "__main__":
    # Create the engine with the data csv
    csv_path = "marketData/jesus_christ.csv"
    engine = BacktestEngine(csv_path, FrequentTradingStrategy, initial_capital=10000.0)
    
    # Run backtest
    engine.run()
    
    # Output to CSV
    engine.output_results(trades_file="trade_ledger.csv", returns_file="returns.csv")
