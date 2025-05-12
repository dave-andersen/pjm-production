import argparse
import polars as pl
import matplotlib.pyplot as plt
# import matplotlib.dates as mdates # Uncomment if more specific date formatting is needed

renewable_types = ["Hydro", "Wind", "Solar", "Storage", "Other Renewables"]

def analyze_production(csv_path):
    try:
        # Use try_parse_dates for automatic datetime conversion
        df = pl.read_csv(csv_path, try_parse_dates=True)
    except Exception as e:
        print(f"Error reading or parsing CSV file: {e}")
        return

    if df.is_empty():
        print("The CSV file is empty or could not be parsed correctly.")
        return

    # Calculate MWh for all fuel types at once
    mwh_per_type = df.group_by("fuel_type").agg(
        pl.sum("mw").alias("total_mw")
    )

    total_mwh = df["mw"].sum()

    # Calculate renewable MWh from the aggregated data
    renewable_mwh_agg = mwh_per_type.filter(
        pl.col("fuel_type").is_in(renewable_types)
    )
    renewable_mwh = renewable_mwh_agg["total_mw"].sum() if not renewable_mwh_agg.is_empty() else 0.0

    print(f"Total generation: {total_mwh} MWh")
    print(f"Renewable generation: {renewable_mwh} MWh")

    if total_mwh > 0:
        percentage_renewable = (renewable_mwh / total_mwh) * 100
        print(f"Percentage renewable: {percentage_renewable:.2f}%")
    else:
        print("Percentage renewable: N/A (total generation is zero)")

    # Print generation by source using the aggregated data
    mwh_lookup = {
        row["fuel_type"]: row["total_mw"]
        for row in mwh_per_type.iter_rows(named=True)
    }

    print("\nGeneration by source:")
    for source in renewable_types:
        if source in mwh_lookup:
            gen = mwh_lookup[source]
            print(f"  - {source} generation: {gen} MWh")

    other_fuel_types = sorted([
        ft for ft in mwh_lookup.keys() if ft not in renewable_types
    ])
    for source in other_fuel_types:
        gen = mwh_lookup[source]
        print(f"  - {source} generation: {gen} MWh")


    def find_peak(input_df, time_col="datetime_beginning_ept", value_col="mw"):
        if input_df.is_empty():
            return "N/A", 0.0

        peak_row_df = input_df.sort(value_col, descending=True).head(1)

        if peak_row_df.is_empty():
            return "N/A", 0.0

        peak_time = peak_row_df[time_col].item()
        peak_value = peak_row_df[value_col].item()
        return peak_time, peak_value

    print("\nPeak Generation Analysis:")
    coal_data = df.filter(pl.col("fuel_type") == "Coal")
    coal_peak_hour, coal_peak_mw = find_peak(coal_data)
    print(f"Peak coal generation: {coal_peak_mw} MW at {coal_peak_hour}")

    renewable_data = df.filter(pl.col("fuel_type").is_in(renewable_types))
    if not renewable_data.is_empty():
        renewable_hourly = renewable_data.group_by("datetime_beginning_ept").agg(
            pl.sum("mw").alias("mw")
        )
        renewable_peak_hour, renewable_peak_mw = find_peak(renewable_hourly)
    else:
        renewable_peak_hour, renewable_peak_mw = "N/A", 0.0

    print(f"Peak renewable generation: {renewable_peak_mw} MW at {renewable_peak_hour}")

def plot_gen(csv_path):
    """
    Generates a plot of energy generation (MW) over time for each fuel type
    and saves it to gen_plot.svg.
    """
    try:
        df = pl.read_csv(csv_path, try_parse_dates=True)
    except Exception as e:
        print(f"Error reading or parsing CSV file for plotting: {e}")
        return

    if df.is_empty():
        print("The CSV file is empty. Cannot generate plot.")
        return

    df = df.with_columns(
        pl.col("datetime_beginning_ept").str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p", strict=False)
    )

    pivoted_df = df.pivot(
        values="mw",
        index="datetime_beginning_ept",
        on="fuel_type"
    ).sort("datetime_beginning_ept")

    fig, ax = plt.subplots(figsize=(18, 9))

    time_col_series = pivoted_df["datetime_beginning_ept"]

    fuel_color_map = {
        "Coal": "black",
        "Solar": "red",
        "Hydro": "darkblue",
        "Wind": "darkgreen",
        "Storage": "lightgreen",
        "Gas": "lightblue",
        "Nuclear": "yellow",
        "Oil": "brown",
        "Other Renewables": "purple",
        "Multiple Fuels": "black",
        "All Renewables": "green",
    }
    fuel_linestyle_map = {
        "Multiple Fuels": "dotted",
        "All Renewables": "dashed",
    }

    actual_renewable_columns = [
        col for col in renewable_types if col in pivoted_df.columns
    ]

    if actual_renewable_columns:
        pivoted_df = pivoted_df.with_columns(
            pl.sum_horizontal(actual_renewable_columns).alias("All Renewables")
        )
    else:
        pivoted_df = pivoted_df.with_columns(
            pl.lit(0).alias("All Renewables")
        )


    for fuel_column_name in pivoted_df.columns:
        if fuel_column_name == "datetime_beginning_ept":
            continue

        mw_series = pivoted_df[fuel_column_name]

        if not mw_series.is_null().all():
            color = fuel_color_map.get(fuel_column_name, "black") # Get color, None if not in map
            linestyle = fuel_linestyle_map.get(fuel_column_name, '-') # Default to solid line

            ax.plot(time_col_series.to_numpy(), mw_series.to_numpy(), label=fuel_column_name, color=color, linestyle=linestyle)


    ax.set_title("Energy Generation Over Time by Fuel Type")
    ax.set_xlabel("Time")
    ax.set_ylabel("Generation (MW)")

    # Place legend outside and adjust layout
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1.0))
    fig.autofmt_xdate() # Improve date formatting
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    # Adjust tight_layout to make space for the legend
    plt.tight_layout(rect=[0, 0, 0.85, 1])


    output_filename = "gen_plot.svg"
    try:
        plt.savefig(output_filename)
        print(f"Plot saved to {output_filename}")
    except Exception as e:
        print(f"Error saving plot: {e}")
    finally:
        plt.close(fig)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze power generation data from PJM")
    parser.add_argument("csv_file", help="Path to the CSV file containing generation data", type=str)
    parser.add_argument("--plot", action="store_true", help="Generate and save a plot of generation over time.")
    args = parser.parse_args()

    analyze_production(args.csv_file)

    if args.plot:
        plot_gen(args.csv_file)
