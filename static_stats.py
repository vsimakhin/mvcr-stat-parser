import json
import os
import shutil
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from tabulate import tabulate
import math

# Constants
DATA_PATH = "./output/parsed_data_raw.json"
STATS_DIR = "./stats"
IMG_DIR = os.path.join(STATS_DIR, "img")
INDEX_FILE = "README.md"

def fmt(n):
    if pd.isna(n):
        return ""
    return f"{int(n):,}".replace(",", " ")

def fmt_with_pct(value, pct):
    if pd.isna(value):
        return ""
    
    number = f"{int(value):,}".replace(",", " ")
    
    if pd.isna(pct) or math.isinf(pct):
        return number
    
    return f"{number} ({pct:+.2f}%)"

def cleanup():
    """Cleanup the stats directory before generating new data."""
    if os.path.exists(STATS_DIR):
        print(f"Cleaning up {STATS_DIR}...")
        shutil.rmtree(STATS_DIR)
    os.makedirs(IMG_DIR, exist_ok=True)
    print(f"Created directory: {IMG_DIR}")

def load_data():
    """Load parsed data from JSON file."""
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Data file not found at {DATA_PATH}")
    with open(DATA_PATH, "r", encoding="utf-8") as file:
        return json.load(file)

def generate_stats():
    """Generate static charts and Markdown files for each country."""
    cleanup()
    
    try:
        data = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    countries = sorted(data.keys())
    index_links = []

    for country in countries:
        # Sanitize country name for filenames
        safe_country = country.replace(" ", "_").replace("/", "_").replace("\\", "_")
        print(f"Generating stats for {country}...")
        
        # Extract data
        dates_raw = []
        total = []
        long = []
        permanent = []
        asyl = []

        country_data = data[country]
        # Sort dates chronologically
        sorted_dates = sorted(country_data.keys(), key=lambda d: pd.to_datetime(d, format="%m.%Y", errors="coerce"))

        for date in sorted_dates:
            dates_raw.append(date)
            total.append(country_data[date].get('total', {}).get('celkem', 0))
            long.append(country_data[date].get('přechodně', {}).get('celkem', 0))
            permanent.append(country_data[date].get('trvale', {}).get('celkem', 0))
            asyl.append(country_data[date].get('dočasná ochrana', {}).get('celkem', 0))

        # Create DataFrame
        df = pd.DataFrame({
            "Date": pd.to_datetime(dates_raw, format="%m.%Y", errors="coerce"),
            "Total": total,
            "Long": long,
            "Permanent": permanent,
            "Asyl": asyl
        }).sort_values("Date")

        # Calculate percentage change (month-to-month)
        df["Total_%"] = df["Total"].pct_change() * 100
        df["Long_%"] = df["Long"].pct_change() * 100
        df["Permanent_%"] = df["Permanent"].pct_change() * 100
        df["Asyl_%"] = df["Asyl"].pct_change() * 100

        # Generate Plotly Chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Total"], mode='lines+markers', name='Total'))
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Long"], mode='lines+markers', name='Longterm residence permit'))
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Permanent"], mode='lines+markers', name='Permanent residence permit'))
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Asyl"], mode='lines+markers', name='Asylum'))

        fig.update_layout(
            title=f"Cizinci s povoleným pobytem - {country}",
            margin=dict(l=40, r=40, t=60, b=40),
            hovermode='closest',
            width=1200,
            height=600,
            template="plotly_white",
            yaxis=dict(tickformat='d')
        )

        img_filename = f"{safe_country}.png"
        img_path = os.path.join(IMG_DIR, img_filename)
        
        try:
            # Write static image
            fig.write_image(img_path)
            chart_embed = f"![Cizinci s povoleným pobytem v ČR {country}](img/{img_filename})\n\n"
        except Exception as e:
            print(f"Warning: Could not save image for {country}: {e}")
            chart_embed = ""

        # Prepare data for tables
        df_display = df.copy()
        df_display['Year'] = df_display['Date'].dt.year
        df_display['Date_str'] = df_display['Date'].dt.strftime('%m.%Y')

        # Prepare summary statistics (min/max per year)
        summary_data = []
        for year, year_df in df_display.groupby('Year'):
            summary_data.append([
                year,
                f"{fmt(year_df['Total'].min())} / {fmt(year_df['Total'].max())}",
                f"{fmt(year_df['Long'].min())} / {fmt(year_df['Long'].max())}",
                f"{fmt(year_df['Permanent'].min())} / {fmt(year_df['Permanent'].max())}",
                f"{fmt(year_df['Asyl'].min())} / {fmt(year_df['Asyl'].max())}"
            ])
        
        summary_md = tabulate(
            summary_data,
            headers=["Year", "Total Min/Max", "Longterm Residence Permit Min/Max", "Permanent Residence Permit Min/Max", "Asylum Min/Max"],
            tablefmt="github",
            disable_numparse=True
        )

        # Prepare detailed Markdown table with year grouping
        table_data = []
        current_year = None
        for _, row in df_display.iterrows():
            year = row['Year']
            if year != current_year:
                # Add a separator/header row for the new year
                table_data.append([f"**{year}**", "", "", "", ""])
                current_year = year
            table_data.append([
                row['Date_str'], 
                fmt_with_pct(row['Total'], row['Total_%']), 
                fmt_with_pct(row['Long'], row['Long_%']), 
                fmt_with_pct(row['Permanent'], row['Permanent_%']), 
                fmt_with_pct(row['Asyl'], row['Asyl_%'])
            ])

        table_md = tabulate(
            table_data, 
            headers=["Date", "Total", "Longterm Residence Permit", "Permanent Residence Permit", "Asylum"], 
            tablefmt="github",
            disable_numparse=True
        )

        # Create Country Markdown File
        md_filename = f"{safe_country}.md"
        md_path = os.path.join(STATS_DIR, md_filename)
        
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(f"# {country}\n\n")
            f.write(chart_embed)
            f.write("## Souhrnné statistiky (Min/Max pro rok) / Summary Statistics (Min/Max per year)\n\n")
            f.write(summary_md)
            f.write("\n\n")
            f.write("## Detailní tabulka / Detailed Data Table\n\n")
            f.write(table_md)
            f.write("\n")

        index_links.append(f"- [{country}](stats/{md_filename})")

    # Generate index file (stats.md in root)
    print(f"Generating root {INDEX_FILE}...")
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        f.write("## Cizinci s povoleným pobytem v ČR/Foreigners in Czech Republic - Statistics Overview\n\n")
        f.write("Data source: https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx\n\n")
        f.write("Tento soubor obsahuje odkazy na detailní statistiky pro každý země/This file contains links to detailed statistics for each country.\n\n")
        f.write("\n".join(index_links))
        f.write("\n")
        f.write("""

### Usage 

You need python installed. Clone or download the repository from github and run:

```bash
pipenv install
pipenv shell
python app.py # for dynamic stats
python static_stats.py # for static stats
```

### Data update

- Download new files from https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx to the `source` folder
- run `python ./parser.py`

        """)
    
    print("Execution completed successfully!")

if __name__ == "__main__":
    generate_stats()
