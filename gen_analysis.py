import argparse
import polars as pl

renewable_types = ["Hydro", "Wind", "Solar", "Storage", "Other Renewables"]

def analyze_production(csv_name):
    df = pl.read_csv(csv_name)
    other_types = df.filter(pl.col("fuel_type").is_in(renewable_types) == False)["fuel_type"].unique().to_list()
    other_types.sort()

    mwh_per_type = df.group_by("fuel_type").agg(
           pl.sum("mw").alias("total_mw")
       )
    total_mwh = mwh_per_type["total_mw"].sum()

    renewable_mwh_agg = mwh_per_type.filter(
        pl.col("fuel_type").is_in(renewable_types)
    )
    renewable_mwh = renewable_mwh_agg["total_mw"].sum() if not renewable_mwh_agg.is_empty() else 0.0

    renewable_data = df.filter(pl.col("fuel_type").is_in(renewable_types))

    print(f"Total generation: {total_mwh} MWh")
    print(f"Renewable generation: {renewable_mwh} MWh")
    if total_mwh > 0:
        percentage_renewable = (renewable_mwh / total_mwh) * 100
        print(f"Percentage renewable: {percentage_renewable:.2f}%")
    else:
        print("Percentage renewable: N/A (total generation is zero)")

    mwh_lookup = {
        row["fuel_type"]: row["total_mw"]
        for row in mwh_per_type.iter_rows(named=True)
    }

    for source in renewable_types + other_types:
        gen = mwh_lookup[source]
        print(f"  - {source} generation: {gen} MWh")

    def find_peak(type_df):
        i = type_df.sort("mw", descending=True).head(1)
        return i["datetime_beginning_ept"].item(), i["mw"].item()

    coal_data = df.filter(pl.col("fuel_type") == "Coal")
    coal_peak_hour, coal_peak_mw = find_peak(coal_data)

    renewable_hourly = renewable_data.group_by("datetime_beginning_ept").agg(pl.sum("mw").alias("mw"))
    renewable_peak_hour, renewable_peak_mw = find_peak(renewable_hourly)

    print(f"Peak coal generation: {coal_peak_mw} MW at {coal_peak_hour}")
    print(f"Peak renewable generation: {renewable_peak_mw} MW at {renewable_peak_hour}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze power generation data from PJM")
    parser.add_argument("csv_file", help="Path to the CSV file containing generation data",
        type=argparse.FileType('rb'))
    args = parser.parse_args()

    analyze_production(args.csv_file)
