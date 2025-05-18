import marimo

__generated_with = "0.13.10"
app = marimo.App(width="full")

with app.setup:
    import marimo as mo
    import polars as pl
    import polars.selectors as cs
    import altair as alt
    from datetime import date, datetime


@app.cell
def _():
    df = (
        pl.read_parquet("data/vesuvius.parquet")
        .filter(pl.col("time").ge(date(2013, 1, 1)))
        .with_columns(
            pl.col("time").dt.year().alias("year"),
            pl.col("time").dt.month().alias("month"),
            pl.col("duration_magnitude_md").exp().round(2).alias("magnitude"),
        )
    )
    df
    return (df,)


@app.cell
def _(df):
    month_map = {
        "1": "January",
        "2": "February",
        "3": "March",
        "4": "April",
        "5": "May",
        "6": "June",
        "7": "July",
        "8": "August",
        "9": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }

    events_by_month = (
        df.group_by("year", "month")
        .agg(
            pl.len(),
            pl.col("magnitude").sum().round(0).alias("sum_mag"),
            pl.col("magnitude").mean().round(2).alias("avg_mag"),
        )
        .with_columns(
            pl.col("len").mul(pl.col("avg_mag")).round(0).alias("danger"),
        )
        .sort("year", "month")
        .with_columns(
            pl.col("month").cast(pl.String).replace(month_map),
        )
    )

    danger_above_200 = events_by_month.filter(pl.col("danger").ge(200))
    max_danger = events_by_month.select("danger").max().item()
    return danger_above_200, events_by_month, max_danger


@app.cell
def _(danger_above_200, events_by_month, max_danger):
    month_list = [
        "December",
        "November",
        "October",
        "September",
        "August",
        "July",
        "June",
        "May",
        "April",
        "March",
        "February",
        "January",
    ]

    heatmap = (
        alt.Chart(events_by_month)
        .mark_rect()
        .encode(
            alt.X("year").type("ordinal").axis(title=None, labelAngle=45),
            alt.Y("month").type("ordinal").sort(month_list).title(None),
            alt.Color("danger")
            .type("quantitative")
            .legend(title="Danger Level")
            # ['blues', 'tealblues', 'teals', 'greens', 'browns', 'greys', 'purples', 'warmgreys', 'reds', 'oranges']
            # ['turbo', 'viridis', 'inferno', 'magma', 'plasma', 'cividis']
            .scale(scheme="magma", domainMin=0, domainMax=max_danger),
        )
        .properties(
            width=400,
            height=400,
            title=alt.Title(
                text='The "Danger" of Vesuvius',
                subtitle=[
                    "Danger (# of events X avg. magnitude (un-logged)) peaked in Dec 2018 (over 300).", 
                    "Spikes tend to occur in Spring and Winter."
                ],
                offset=10,
            ),
        )
    )

    text = (
        alt.Chart(danger_above_200)
        .mark_text(align="center", baseline="middle", fontSize=10, color="black")
        .encode(
            alt.X("year").type("ordinal"),
            alt.Y("month").type("ordinal").sort(month_list),
            alt.Text("danger").type("quantitative"),
        )
    )

    footer_df = pl.from_dict(
        {
            "text": [
                "Data Source: INGV | Chart by Ozan Ozbeker | TidyTuesday 2025 / Week 19"
            ]
        }
    )

    footer = alt.Chart(footer_df).mark_text(align="left").encode(text="text:N")

    chart = alt.vconcat(heatmap + text, footer).configure_concat(spacing=5)
    return (chart,)


@app.cell
def _(chart):
    chart
    return


@app.cell
def _(chart):
    chart.save(fp="charts/2025-05-13_danger-of-vesuvius.png", ppi=600)
    return


if __name__ == "__main__":
    app.run()
