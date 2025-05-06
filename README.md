# PJM Power Production Analysis

This utility analyzes power generation data from PJM Interconnection to show the total generation over the period of the data and breaking it out by renewable/non-renewable and type. It also outputs the peak coal and renewable generation.

## Prerequisites

- Python 3.7 or higher
- Required packages:
  - polars
  - argparse

Install the required packages or run with uv:

```bash
pip install polars
```

using [uv](https://github.com/astral-sh/uv),

```
  uv run --with polars gen_analysis.py <CSV_FILE>
```

## Data Source

Before running the script, you must download the required CSV data:

1. Visit the [PJM Data Miner website](https://dataminer2.pjm.com/feed/gen_by_fuel)
2. Select your desired date range and parameters
3. Download the data in CSV format
4. Save the CSV file to your project directory

## Usage

Run the script with the path to your downloaded CSV file:

```bash
python gen_analysis.py path/to/your_pjm_data.csv
```

or with uv:

```
  uv run --with polars gen_analysis.py path/to/your_pjm_data.CSV
```

## Example Output (April 2025)

```
Total generation: 61030280 MWh
Renewable generation: 7294899 MWh
Percentage renewable: 11.95%
  - Hydro generation: 1270332 MWh
  - Wind generation: 3249044 MWh
  - Solar generation: 2364048 MWh
  - Storage generation: 1379 MWh
  - Other Renewables generation: 410096 MWh
  - Oil generation: 306782 MWh
  - Gas generation: 22959140 MWh
  - Coal generation: 9549318 MWh
  - Multiple Fuels generation: 564518 MWh
  - Nuclear generation: 20355623 MWh
Peak coal generation: 18460.00 MWh at 4/4/2025 3:00:00 PM
Peak renewable generation: 21228.00 MWh at 4/18/2025 1:00:00 PM
```
