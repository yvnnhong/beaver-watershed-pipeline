#CELL 1: fetch beaver data from GBIF API
import requests

url = "https://api.gbif.org/v1/occurrence/search"
params = {
    "taxonKey": 2439838,  # Castor canadensis (North American Beaver) - found via gbif.org/species/2439838
    "country": "US",
    "limit": 5
}

response = requests.get(url, params=params)
data = response.json()  # .json() converts the JSON response into a Python dictionary
print(data)

#CELL 2: load into pandas DataFrame and preview
import pandas as pd

records = data['results']  # 'results' is the key in the GBIF response that holds the actual occurrence records
df = pd.DataFrame(records)  # pd.DataFrame converts a list of dicts into a table - each dict = one row, keys = column names

# [[ ]] double brackets: outer [] selects columns from DataFrame, inner [] is a Python list of column names
df[['species', 'decimalLatitude', 'decimalLongitude', 'year', 'month', 'day', 'stateProvince', 'country']].head()  # .head() shows first 5 rows

#CELL 3: fetch 5,100 US beaver records using pagination
# GBIF API max limit is 300 per request, so we loop with offset to paginate
# pagination = fetching data in pages/batches because APIs won't return everything at once
all_records = []

for offset in range(0, 5000, 300):  # range(start, stop, step) - fetches batches of 300 up to 5000
    response = requests.get(url, params={
        "taxonKey": 2439838,
        "country": "US",
        "limit": 300,
        "offset": offset,  # offset tells the API where to start - like skipping to page N
        "hasCoordinate": True  # only return records that have lat/lon coordinates (needed for spatial join)
    })
    batch = response.json()['results']  # batch is type list[dict] - each dict is one beaver occurrence record
    all_records.extend(batch)  # extend adds all items from batch into all_records (vs append which adds the list itself)
    print(f"Fetched {len(all_records)} records so far...")

df_beavers = pd.DataFrame(all_records)
print(f"Total: {len(df_beavers)} beaver records")

#CELL 4: clean the DataFrame - keep only useful columns
df_beavers = df_beavers[[
    'species',
    'decimalLatitude',
    'decimalLongitude',
    'year',
    'month',
    'day',
    'stateProvince',
    'country'
]].dropna(subset=['decimalLatitude', 'decimalLongitude'])  # dropna removes rows missing coordinates since we need them for spatial join

print(df_beavers.shape)  # shape returns (rows, columns) tuple
df_beavers.head()

#CELL 5: save to CSV so we don't have to re-fetch every time
df_beavers.to_csv('beavers_us.csv', index=False)  # index=False means don't save row numbers as a column
print("Saved!")

#CELL 6: fetch dissolved oxygen data from USGS Water Services API
# Note: we tried EPA Water Quality Portal (waterqualitydata.us) first but it kept returning 500 errors for large queries
# USGS Water Services is more reliable - switched to this instead
# parameterCd=00300 is USGS's internal code for dissolved oxygen
usgs_url = "https://waterservices.usgs.gov/nwis/iv/"

usgs_params = {
    "format": "json",
    "stateCd": "CA",        # California only for prototype (full US in production Lambda)
    "parameterCd": "00300", # 00300 = dissolved oxygen parameter code
    "siteType": "ST",       # ST = Stream sites only
    "period": "P365D"       # last 365 days of data
}

usgs_response = requests.get(usgs_url, params=usgs_params, timeout=60)
print(usgs_response.status_code)  # 200 = success

#CELL 7: parse the USGS JSON response into a DataFrame
# USGS response structure: response -> 'value' -> 'timeSeries' -> list of 72 stations
# each station has 'sourceInfo' (location data) and 'values' (DO readings over time)
usgs_data = usgs_response.json()  # converts JSON to Python dict
time_series = usgs_data['value']['timeSeries']  # list of 72 stream monitoring stations
print(f"Number of stations: {len(time_series)}")

records: list[dict] = []  # type hint: list of dicts, one dict per reading
for station in time_series:
    site_info = station['sourceInfo']
    lat = site_info['geoLocation']['geogLocation']['latitude']   # geoLocation = outer wrapper, geogLocation = geographic location specifically
    lon = site_info['geoLocation']['geogLocation']['longitude']
    site_name = site_info['siteName']

    # station['values'][0]['value'] - 'values' is a list, [0] gets first entry, ['value'] gets the readings list inside it
    for reading in station['values'][0]['value']:
        records.append({
            'site_name': site_name,      # key names on left are chosen by us - just descriptive labels
            'latitude': lat,             # values on right come from the USGS API response
            'longitude': lon,
            'datetime': reading['dateTime'],
            'dissolved_oxygen': reading['value']
        })

df_water = pd.DataFrame(records)
print(df_water.shape)
df_water.head()

#CELL 8: save water quality data to CSV
df_water.to_csv('water_quality_ca.csv', index=False)
print("Saved!")

#CELL 9: spatial join - match each beaver sighting to its nearest water quality station
# BallTree is a data structure for fast nearest neighbor lookup
# O(log n) time complexity vs O(n) brute force - much faster for large datasets
# it's NOT ML - it's a spatial data structure, similar concept to binary search trees but in geographic space
from sklearn.neighbors import BallTree
import numpy as np

# get unique stations only - we have 938k readings but only 41 unique station locations
df_stations = df_water[['site_name', 'latitude', 'longitude']].drop_duplicates()
print(f"Number of unique stations: {len(df_stations)}")

# BallTree requires coordinates in radians (not degrees) when using haversine distance metric
station_coords = np.radians(df_stations[['latitude', 'longitude']].values)
tree = BallTree(station_coords, metric='haversine')  # haversine = correct distance formula for lat/lon on a sphere

# filter beavers to California only - our water stations are CA only so US beavers gave 2347km avg distance
df_beavers_ca = df_beavers[df_beavers['stateProvince'] == 'California']
print(f"California beaver sightings: {len(df_beavers_ca)}")

beaver_coords_ca = np.radians(df_beavers_ca[['decimalLatitude', 'decimalLongitude']].values)

# k=1 means find the 1 nearest neighbor (closest station) for each beaver sighting
distances, indices = tree.query(beaver_coords_ca, k=1)
distances_km = distances * 6371  # convert radians to km (Earth radius = 6371 km)

print(f"Average distance to nearest station: {distances_km.mean():.1f} km")
print(f"Max distance: {distances_km.max():.1f} km")
print(f"Min distance: {distances_km.min():.1f} km")

#CELL 10: assemble the final joined DataFrame
# reset_index(drop=True): when filtering a DataFrame, row numbers keep original values (e.g. 47,103,205)
# reset_index resets them back to 0,1,2,3 so indices align correctly for the join
df_beavers_ca = df_beavers_ca.reset_index(drop=True)

# indices.flatten(): BallTree returns 2D array [[0],[5],[12]], flatten() converts to 1D [0,5,12]
nearest_station_idx = indices.flatten()

# iloc selects rows by position number - grabs the matched station row for each beaver sighting
nearest_stations = df_stations.iloc[nearest_station_idx].reset_index(drop=True)

# pd.concat glues DataFrames together - axis=1 means side by side (horizontal), axis=0 would stack rows (vertical)
df_joined = pd.concat([
    df_beavers_ca,
    nearest_stations.rename(columns={  # rename so we know these columns came from station data
        'site_name': 'nearest_station',
        'latitude': 'station_lat',
        'longitude': 'station_lon'
    }),
    pd.Series(distances_km.flatten(), name='distance_km')  # pd.Series converts numpy array to a pandas column
], axis=1)

print(df_joined.shape)
df_joined.head()

#CELL 11: add average dissolved oxygen per station to the joined DataFrame
# dissolved_oxygen came in as strings from USGS API - need to convert to float for math
df_water['dissolved_oxygen'] = pd.to_numeric(df_water['dissolved_oxygen'], errors='coerce')  # errors='coerce' turns bad values into NaN instead of crashing

# groupby groups rows by station name, then .mean() calculates average DO for each station
avg_do = df_water.groupby('site_name')['dissolved_oxygen'].mean().reset_index()
avg_do.columns = ['nearest_station', 'avg_dissolved_oxygen']

# merge joins two DataFrames - on='nearest_station' is the matching key (like SQL JOIN ON)
# how='left' keeps all rows from df_joined even if no match found in avg_do
df_final = df_joined.merge(avg_do, on='nearest_station', how='left')

print(df_final.shape)
df_final[['species', 'decimalLatitude', 'decimalLongitude', 'nearest_station', 'distance_km', 'avg_dissolved_oxygen']].head()

#CELL 12: check for trends in the data
print(df_final['nearest_station'].value_counts())
print(f"\nAvg dissolved oxygen across all matched stations: {df_final['avg_dissolved_oxygen'].mean():.2f}")
print(f"Min: {df_final['avg_dissolved_oxygen'].min():.2f}")
print(f"Max: {df_final['avg_dissolved_oxygen'].max():.2f}")

#CELL 13: visualize distance vs water quality
import matplotlib.pyplot as plt

plt.scatter(df_final['distance_km'], df_final['avg_dissolved_oxygen'], alpha=0.5)
plt.xlabel('Distance to Nearest Station (km)')
plt.ylabel('Avg Dissolved Oxygen (mg/L)')
plt.title('Beaver Sightings: Distance vs Water Quality')
plt.show()

#CELL 14: visualize water quality by watershed (more meaningful chart)
station_summary = df_final.groupby('nearest_station').agg(
    beaver_count=('species', 'count'),       # count beaver sightings per station
    avg_do=('avg_dissolved_oxygen', 'mean')  # average dissolved oxygen per station
).reset_index().sort_values('beaver_count', ascending=False)

plt.figure(figsize=(12, 6))
plt.barh(station_summary['nearest_station'], station_summary['avg_do'])
plt.xlabel('Avg Dissolved Oxygen (mg/L)')
plt.title('Water Quality at Stations Near Beaver Activity')
plt.tight_layout()
plt.show()

#CELL 15: save final joined dataset
df_final.to_csv('beaver_water_joined.csv', index=False)
print("Saved! Final dataset: 260 beaver sightings x 13 columns")
