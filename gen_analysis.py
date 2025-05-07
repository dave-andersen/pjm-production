import polars as pl
import argparse

renewable_types = ["Hydro", "Wind", "Solar", "Storage", "Other Renewables"]

def mwh_by_type(df, fuel_type):
    return df.filter(pl.col("fuel_type") == fuel_type)["mw"].sum()

def analyze_production(csv_name):
    df = pl.read_csv(csv_name)
    other_types = df.filter(pl.col("fuel_type").is_in(renewable_types) == False)["fuel_type"].unique().to_list()

    total_mwh = df["mw"].sum()

    renewable_data = df.filter(pl.col("fuel_type").is_in(renewable_types))
    renewable_mwh = renewable_data["mw"].sum()

    coal_mwh = mwh_by_type(df, "Coal")

    print(f"Total generation: {total_mwh} MWh")
    print(f"Renewable generation: {renewable_mwh} MWh")
    print(f"Percentage renewable: {(renewable_mwh/total_mwh)*100:.2f}%")
    for source in renewable_types + other_types:
        gen = mwh_by_type(df, source)
        print(f"  - {source} generation: {gen} MWh")

    # Find peak coal generation
    coal_data = df.filter(pl.col("fuel_type") == "Coal")
    coal_peak = coal_data.sort("mw", descending=True).head(1)
    coal_peak_hour = coal_peak["datetime_beginning_ept"].item()
    coal_peak_mw = coal_peak["mw"].item()

    # Find peak renewable generation
    renewable_hourly = renewable_data.group_by("datetime_beginning_ept").agg(pl.sum("mw").alias("total_mw"))
    renewable_peak = renewable_hourly.sort("total_mw", descending=True).head(1)
    renewable_peak_time = renewable_peak["datetime_beginning_ept"].item()
    renewable_peak_mw = renewable_peak["total_mw"].item()

    print(f"Peak coal generation: {coal_peak_mw} MW at {coal_peak_hour}")
    print(f"Peak renewable generation: {renewable_peak_mw} MW at {renewable_peak_time}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze power generation data from PJM")
    parser.add_argument("csv_file", help="Path to the CSV file containing generation data")
    args = parser.parse_args()

    analyze_production(args.csv_file)
