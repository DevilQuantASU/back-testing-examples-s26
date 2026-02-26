import backtrader as bt
import pandas as pd
import datetime

class SmaCross(bt.Strategy):
    # Strategy parameters
    params = dict(
        pfast=10,  # Fast moving average
        pslow=30   # Slow moving average
    )

    def __init__(self):
        # We process multiple data feeds (AAPL and MSFT)
        self.fast_smas = {data: bt.ind.SMA(data.close, period=self.p.pfast) for data in self.datas}
        self.slow_smas = {data: bt.ind.SMA(data.close, period=self.p.pslow) for data in self.datas}

    def next(self):
        for data in self.datas:
            pos = self.getposition(data)
            
            fast = self.fast_smas[data][0]
            slow = self.slow_smas[data][0]

            # Not in the market
            if not pos:
                if fast > slow:  # Fast above Slow
                    self.buy(data=data, size=50) # Buy 50 shares
            # Already in the market
            elif fast < slow:    # Fast below Slow
                self.close(data=data)       # Close position

def run_backtest():
    cerebro = bt.Cerebro()

    # Define the tickers we want backtrader to ingest
    tickers = ["AAPL", "MSFT"]
    
    for ticker in tickers:
        # Load CSV using GenericCSVData feed mapping directly to the yfinance columns
        data = bt.feeds.GenericCSVData(
            dataname=f"{ticker}.csv",
            dtformat='%Y-%m-%d',
            datetime=0,
            close=1,
            high=2,
            low=3,
            open=4,
            volume=5,
            openinterest=-1,
            fromdate=datetime.datetime(2020, 1, 1),
            todate=datetime.datetime(2023, 12, 31),
            reverse=False
        )
        # Add the data feed to cerebro
        cerebro.adddata(data)

    # Add the strategy
    cerebro.addstrategy(SmaCross)
    
    # Set our starting cash (large enough for 50 shares of AAPL + MSFT simultaneously)
    cerebro.broker.setcash(100000.0)

    # Add the TimeReturn analyzer to generate quantstats-compatible returns
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='returns')

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # Run the backtest
    results = cerebro.run()
    strat = results[0]
    
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Export Returns for Quantstats Compatibility
    # The TimeReturn analyzer returns a dictionary: {datetime.date: float_return}
    returns_dict = strat.analyzers.returns.get_analysis()
    
    # Convert dict to pandas DataFrame
    df = pd.DataFrame(list(returns_dict.items()), columns=['date', 'returns'])
    
    # Format date string to match quantstats expectation
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df.to_csv('returns.csv', index=False)
    
    print("Saved daily returns to returns.csv")

if __name__ == '__main__':
    run_backtest()
