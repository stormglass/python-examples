import os
import datetime
import multiprocessing

import requests
import numpy
import tqdm

session = None

start = datetime.datetime.utcnow()
end = start + datetime.timedelta(hours=1)


def set_global_session():
    global session
    if not session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": os.environ["STORMGLASS_API_KEY"]
            }
        )


def fetch_coordinate(coordinate):
    latitude, longitude = coordinate

    with session.get(
        "https://api.stormglass.io/v1/weather/point",
        params={
            "lat": latitude,
            "lng": longitude,
            "start": str(start),
            "end": str(end),
            "params": (
                "currentDirection,currentSpeed,precipitation,visibility,swellDirection,swellHeight,swellPeriod,waterTemperature,waveDirection,waveHeight,wavePeriod,windWaveDirection,windWaveHeight,windWavePeriod,seaLevel,windDirection,windSpeed,gust"
            ),
        },
    ) as response:
        data = response.json()
        return (latitude, longitude, data["hours"][0])


def fetch_all_coordinates(coordinates):
    with multiprocessing.Pool(initializer=set_global_session) as pool:
        results = list(
            tqdm.tqdm(
                pool.imap(fetch_coordinate, coordinates), total=len(coordinates)
            )
        )

        return results


if __name__ == "__main__":
    bottom_lat = 55
    top_lat = 59
    right_lng = 9
    left_lng = 3
    resolution = 0.5  # Set to eg. 2, 1, 0.5, 0.25, 0.125 depending on what you need

    coordinates = []
    lats = sorted(
        list(
            numpy.arange(bottom_lat, (top_lat + resolution), resolution, float)
        )
    )
    lngs = sorted(
        list(
            numpy.arange(left_lng, (right_lng + resolution), resolution, float)
        )
    )

    for latitude in lats:
        for longitude in lngs:
            coordinates.append((latitude, longitude))

    print(f"Fetch {len(coordinates)} locations")
    results = fetch_all_coordinates(coordinates)  # `results` is a list of tuples (lat, lng, hour_dict)