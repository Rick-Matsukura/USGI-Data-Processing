import warnings
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from time import time
from datetime import datetime, timedelta

# Suppress FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# USGS Earthquake API endpoint
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# Function to fetch earthquake data for a specific time range
def fetch_earthquake_data(starttime, endtime):
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed request with params: {params}")
        raise Exception(f"API request failed with status code {response.status_code}")

# Initialise an empty list to hold all earthquake data
all_earthquake_list = []

# Fetch data month by month
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
current_date = start_date

while current_date <= end_date:
    next_date = current_date + timedelta(days=30)
    start_date_str = current_date.strftime('%Y-%m-%d')
    end_date_str = next_date.strftime('%Y-%m-%d')
    try:
        data = fetch_earthquake_data(start_date_str, end_date_str)
        for feature in data['features']:
            properties = feature['properties']
            geometry = feature['geometry']
            earthquake = {
                'time': properties['time'],
                'place': properties['place'],
                'magnitude': properties['mag'],
                'longitude': geometry['coordinates'][0],
                'latitude': geometry['coordinates'][1],
                'depth': geometry['coordinates'][2],
                'tsunami': properties.get('tsunami'),
                'title': properties.get('title'),
                'updated': properties.get('updated'),
                'tz': properties.get('tz'),
                'url': properties.get('url'),
                'detail': properties.get('detail'),
                'felt': properties.get('felt'),
                'cdi': properties.get('cdi'),
                'mmi': properties.get('mmi'),
                'alert': properties.get('alert'),
                'status': properties.get('status'),
                'sig': properties.get('sig'),
                'net': properties.get('net'),
                'code': properties.get('code'),
                'ids': properties.get('ids'),
                'sources': properties.get('sources'),
                'types': properties.get('types'),
                'nst': properties.get('nst'),
                'dmin': properties.get('dmin'),
                'rms': properties.get('rms'),
                'gap': properties.get('gap'),
                'magType': properties.get('magType'),
                'type': properties.get('type'),
                'id': feature['id']
            }
            all_earthquake_list.append(earthquake)
    except Exception as e:
        print(f"Skipping failed request from {start_date_str} to {end_date_str}: {e}")
    current_date = next_date

# Record start time for processing time latency
processing_start_time = time()

# Create a pandas DataFrame
earthquake_df = pd.DataFrame(all_earthquake_list)

# Convert the time from milliseconds to datetime
earthquake_df['time'] = pd.to_datetime(earthquake_df['time'], unit='ms')

# Display the pandas DataFrame as a table
print(earthquake_df)

# Scatter Plot (Separate Figure)
fig2, ax2 = plt.subplots(figsize=(18, 6))
scatter = ax2.scatter(earthquake_df['longitude'], earthquake_df['latitude'], c=earthquake_df['magnitude'], cmap='viridis', alpha=0.5, label='Earthquakes')

# Filter for tsunami events
tsunami_events = earthquake_df[earthquake_df['tsunami'] == 1]

# Plot tsunami events
scatter_tsunami = ax2.scatter(tsunami_events['longitude'], tsunami_events['latitude'], c='red', alpha=0.6, label='Tsunami Events')
fig2.colorbar(scatter, ax=ax2, label='Magnitude')
ax2.set_title('Scatter Plot of Earthquake Locations')
ax2.set_xlabel('Longitude')
ax2.set_ylabel('Latitude')
ax2.legend()
plt.tight_layout()
plt.show()

# Display Tsunami Events in a Table
if not tsunami_events.empty:
    print("Tsunami Events Data:")
    print(tsunami_events[['time', 'place', 'magnitude', 'longitude', 'latitude', 'depth']].to_string(index=False))
else:
    print("No Tsunami Events Found")

# Insert a blank line
print("\n")

# Record end time for processing time latency
processing_end_time = time()
processing_time_latency = processing_end_time - processing_start_time

# Print processing time latency
print(f"Processing Time Latency: {processing_time_latency:.2f} seconds")

# Calculate and print the size of the data
memory_usage = earthquake_df.memory_usage(deep=True).sum() / (1024 ** 2)  # Convert bytes to megabytes
print(f"Data Size: {memory_usage:.2f} MB")

# Calculate and print the throughput
throughput = memory_usage / processing_time_latency  # MB per second
print(f"Throughput: {throughput:.2f} MB/s")