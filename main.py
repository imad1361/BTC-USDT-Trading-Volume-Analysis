import pandas as pd
import ccxt
import numpy as np
import matplotlib.pyplot as plt
import time

# Initialize the Binance exchange
exchange = ccxt.binance({
    'enableRateLimit': True,
})

# Define the ticker for Bitcoin perpetual futures
TICKER = 'BTC/USDT'

# Define the period for the last two years
end_date = pd.Timestamp.now()  # Current date and time
start_date = end_date - pd.DateOffset(years=2)

# Function to fetch historical OHLCV data in chunks
def fetch_historical_data(ticker, since, timeframe='5m'):
    all_data = []
    while since < int(end_date.timestamp() * 1000):
        data = exchange.fetch_ohlcv(ticker, timeframe=timeframe, since=since)
        if not data:
            break
        all_data += data
        since = data[-1][0] + 1
        time.sleep(exchange.rateLimit / 1000)
    return all_data

# Fetching 5-minute historical data
data = fetch_historical_data(TICKER, int(start_date.timestamp() * 1000))

# Convert to DataFrame
df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
df.set_index('timestamp', inplace=True)
df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

print(df.head())
print(f"Total rows fetched: {len(df)}")

# Resampling to daily volume
daily_volume = df['Volume'].resample('D').sum()

# Adding a column for day of the week (0=Monday, 6=Sunday)
daily_volume_df = daily_volume.reset_index()
daily_volume_df['Day'] = daily_volume_df['timestamp'].dt.day_name()

# Grouping by day of the week and summing volumes
weekly_volume = daily_volume_df.groupby('Day')['Volume'].sum()

# Finding the day with the highest volume
max_day = weekly_volume.idxmax()
max_day_value = weekly_volume.max()

print(f"Day with highest volume: {max_day} with volume: {max_day_value}")

# Plotting weekly volume distribution
plt.figure(figsize=(10, 6))
weekly_volume.plot(kind='bar', color='lightblue')
plt.axhline(y=max_day_value, color='r', linestyle='--', label='Max Volume')
plt.title('Total Trading Volume of BTC/USDT by Day of Week Over Last Two Years')
plt.xlabel('Day of Week')
plt.ylabel('Total Volume')
plt.xticks(rotation=45)
plt.legend()
plt.grid()
plt.tight_layout()
plt.show()

# Resampling to hourly volume
hourly_volume = df['Volume'].resample('H').sum()

# Adding a column for hour of the day (0-23)
hourly_volume_df = hourly_volume.reset_index()
hourly_volume_df['Hour'] = hourly_volume_df['timestamp'].dt.hour

# Grouping by hour and summing volumes
hourly_total_volume = hourly_volume_df.groupby('Hour')['Volume'].sum()

# Finding the hour with the highest volume
max_hour = hourly_total_volume.idxmax()
max_hour_value = hourly_total_volume.max()

print(f"Hour with highest volume: {max_hour}:00 with volume: {max_hour_value}")

# Plotting hourly volume distribution
plt.figure(figsize=(10, 6))
hourly_total_volume.plot(kind='bar', color='lightgreen')
plt.axhline(y=max_hour_value, color='r', linestyle='--', label='Max Hourly Volume')
plt.title('Total Trading Volume of BTC/USDT by Hour of Day Over Last Two Years')
plt.xlabel('Hour of Day')
plt.ylabel('Total Volume')
plt.xticks(range(0, 24))
plt.legend()
plt.grid()
plt.tight_layout()
plt.show()
