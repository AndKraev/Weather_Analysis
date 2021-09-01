# WEATHER ANALYSIS

Console utility for multithreaded data processing, accumulation of results via
APIs from the Internet and their further representation on graphs. Accepts input data
as a path to a folder with zip archives containing files in the format `.CSV` containing 
a collection with lines with information about hotels.
The received data is stored in the output directory with the following structure:
`{output_folder}\{country}\{city}\`.  
___
## Step-by-Step  
1. Finds all zip files in the input directory and unzips them to get all `.CSV` files.
2. Clears rows from invalid and missing data. 
3. Find cities with the most hotels, others are thrown.
4. Calculates city centers for these cities equidistant to all hotels in the city
5. Fetches minimum and maximum temperature data for each city center from 
[OpenWeather](https://openweathermap.org):
    * historical for the last 5 days;
    * forecast for the next 5 days;
    * current values.
6. Creates temperature graphs for each city center.
7. From all city centers weathers find and writes the following data to JSON file:
   * a city and date with maximum temperature for the period;
   * a city and date with minimum temperature for the period;
   * the city with the maximum change in the maximum temperature;
   * the city and the day with the maximum difference between the maximum and minimum 
     temperature.
8. Fetches addresses for all hotels for these cities.
9. Saves in the directory with the specified structure for each city:
   * all temperature graphs;
   * hotel list in .CSV format containing:
     * hotel name,
     * address,
     * latitude,
     * longitude;
   * received data about city centers into `analysis.json`.

____


## Requirements

* Python 3.7 or above
* Pip installed packages from requirement.txt, please use the following console command:
    `pip install -r /path/to/requirements.txt`
* OpenWeather API key which can be obtained: https://openweathermap.org
* PickPoint API key which can be obtained: https://pickpoint.io
___
## Installation

Please set API keys for [OpenWeather](https://openweathermap.org) and 
[PickPoint](https://pickpoint.io) in Setup.py.

```buildoutcfg
# API key for using PickPoint geocode
pickpoint_api = {YOU API KEY MUST BE HERE}

# API key for using OpenWeather data
openweather_api = {YOU API KEY MUST BE HERE}
```
___
## Usage

On command line, type in the following command for Windows:

    python main.py {path to your input directory}

Optional arguments:

* `--outdir` - path to an output directory, creates a folder named "Output" in the input directory 
if not set.
* `--threads` - a number of threads that will be used while fetching a data from servers
  (default is 1000).
* `--hotels` - a maximum number of hotels that will be written to output CSV files 
(default is 100) 
___
## Directory Structure
    
    tests/                  tests of the core 
    main.py                 contains a parser to run this script
    README.md               this file
    Services.py             service classes
    Setup.py                API keys
    WeatherAnalysis.py      business logic class and dataclasses