from flask import Flask, render_template, request
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import threading
import time
from twilio.rest import Client
import keys  # Assuming keys.py contains your Twilio credentials

app = Flask(__name__)

# Database connection parameters
CONNECTION = {
    'dbname': 'tsdb',
    'user': 'tsdbadmin',
    'password': 'zi8quozcfgpjegv3',
    'host': 'ril4lqv5gq.w35fee3qwl.tsdb.cloud.timescale.com',
    'port': '35435'
}

# Twilio client setup
client = Client(keys.account_sid, keys.auth_token)

# Function to send an alert via Twilio
def send_alert(alert, alert_type):
    client.messages.create(
        body=alert,
        from_=keys.twilio_number,
        to=keys.target_number
    )

# Function to monitor alerts
def monitor_alerts():
    sent_alerts = set()
    while True:
        df, water_level_alerts, battery_level_alerts = fetch_paginated_data_and_alerts(100, 0, 'dwlr1')

        for alert in water_level_alerts:
            if alert not in sent_alerts:
                send_alert(alert, "Water Level")
                sent_alerts.add(alert)

        for alert in battery_level_alerts:
            if alert not in sent_alerts:
                send_alert(alert, "Battery Level")
                sent_alerts.add(alert)

        time.sleep(60)  # Check every 60 seconds

# Function to fetch paginated data and generate alerts
def fetch_paginated_data_and_alerts(limit, offset, table_name, start_date=None, end_date=None):
    conn = psycopg2.connect(**CONNECTION)

    # Prepare date filters for SQL query
    date_filter = ""
    if start_date and end_date:
        date_filter = f"AND \"Timestamp\" BETWEEN '{start_date}' AND '{end_date}'"

    query = f"""
        SELECT * FROM {table_name}
        WHERE TRUE {date_filter}
        ORDER BY "Timestamp"
        LIMIT {limit} OFFSET {offset};
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Generate alerts based on water level and battery level
    water_level_alerts = []
    battery_level_alerts = []

    for index, row in df.iterrows():
        if row["Water Level (m)"] < 3 or row["Water Level (m)"] >= 5:
            water_level_alerts.append(
                f"Critical Zone Alert at {row['Timestamp']}: Water level is {row['Water Level (m)']} meters.")
        if row["Battery Level (%)"] < 20:
            battery_level_alerts.append(
                f"Low Battery Alert at {row['Timestamp']}: Battery level is {row['Battery Level (%)']}%. Please charge the battery.")

    return df, water_level_alerts, battery_level_alerts

# Function to create figures with hover and trend analysis
def create_figures_with_hover(df):
    # Water Level Bar Chart
    water_level_colors = ['red' if level >= 10 else 'blue' for level in df['Water Level (m)']]
    pressure_level = ['red' if level >=103000 else 'blue' for level in df['Pressure (Pa)']]
    temperature_level = ['red' if level >= 70 else 'blue' for level in df['Temperature (°C)']]
    bar_fig = go.Figure()
    bar_fig.add_trace(go.Bar(
        x=df['Timestamp'],
        y=df['Water Level (m)'],
        text=df['Water Level (m)'],
        marker_color=water_level_colors,
        name='Water Level (m)'
    ))
    bar_fig.update_layout(
        yaxis=dict(
            dtick=0.2,
            title='Water Level (m)',
            tickformat='.1f',
            range=[4, df['Water Level (m)'].max() + 1]
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Water Level Bar Chart'
    )

    # Temperature Line Chart
    temp_fig = go.Figure()
    temp_fig.add_trace(go.Scatter(
        x=df['Timestamp'],
        y=df['Temperature (°C)'],
        marker_color=temperature_level,
        mode='lines+markers',
        name='Temperature (°C)'
    ))
    temp_fig.update_layout(
        yaxis=dict(
            title='Temperature (°C)'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Temperature Over Time'
    )

    # Pressure Bar Chart
    pressure_fig = go.Figure()
    pressure_fig.add_trace(go.Bar(
        x=df['Timestamp'],
        y=df['Pressure (Pa)'],
        marker_color=pressure_level,
        name='Pressure(hPa)'

    ))
    pressure_fig.update_layout(
        yaxis=dict(
            title='Pressure(Pa)'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Pressure Over Time'
    )

    # Z-Score Line Chart for Water Level
    z_scores_water_level = (df['Water Level (m)'] - df['Water Level (m)'].mean()) / df['Water Level (m)'].std()
    z_score_fig_water_level = go.Figure()
    z_score_fig_water_level.add_trace(go.Scatter(
        x=df['Timestamp'],
        y=z_scores_water_level,
        mode='lines+markers',
        name='Z-Score (Water Level)'
    ))
    z_score_fig_water_level.update_layout(
        yaxis=dict(
            title='Z-Score'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Z-Score of Water Level'
    )

    # IQR Box Plot for Water Level
    iqr_fig_water_level = go.Figure()
    iqr_fig_water_level.add_trace(go.Box(
        y=df['Water Level (m)'],
        name='Water Level (m)',
        boxpoints='all',
        jitter=0.3,
        pointpos=-1.8
    ))
    iqr_fig_water_level.update_layout(
        yaxis=dict(
            title='Water Level (m)'
        ),
        xaxis=dict(
            title='Distribution'
        ),
        title='IQR of Water Level'
    )

    # Z-Score Line Chart for Temperature
    z_scores_temp = (df['Temperature (°C)'] - df['Temperature (°C)'].mean()) / df['Temperature (°C)'].std()
    z_score_fig_temp = go.Figure()
    z_score_fig_temp.add_trace(go.Scatter(
        x=df['Timestamp'],
        y=z_scores_temp,
        mode='lines+markers',
        name='Z-Score (Temperature)'
    ))
    z_score_fig_temp.update_layout(
        yaxis=dict(
            title='Z-Score'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Z-Score of Temperature'
    )

    # IQR Box Plot for Temperature
    iqr_fig_temp = go.Figure()
    iqr_fig_temp.add_trace(go.Box(
        y=df['Temperature (°C)'],
        name='Temperature (°C)',
        boxpoints='all',
        jitter=0.3,
        pointpos=-1.8
    ))
    iqr_fig_temp.update_layout(
        yaxis=dict(
            title='Temperature (°C)'
        ),
        xaxis=dict(
            title='Distribution'
        ),
        title='IQR of Temperature'
    )

    # Z-Score Line Chart for Pressure
    z_scores_pressure = (df['Pressure (Pa)'] - df['Pressure (Pa)'].mean()) / df['Pressure (Pa)'].std()
    z_score_fig_pressure = go.Figure()
    z_score_fig_pressure.add_trace(go.Scatter(
        x=df['Timestamp'],
        y=z_scores_pressure,
        mode='lines+markers',
        name='Z-Score (Pressure)'
    ))
    z_score_fig_pressure.update_layout(
        yaxis=dict(
            title='Z-Score'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Z-Score of Pressure'
    )

    # IQR Box Plot for Pressure
    iqr_fig_pressure = go.Figure()
    iqr_fig_pressure.add_trace(go.Box(
        y=df['Pressure (Pa)'],
        name='Barometric Pressure(hPa)',
        boxpoints='all',
        jitter=0.3,
        pointpos=-1.8
    ))
    iqr_fig_pressure.update_layout(
        yaxis=dict(
            title='Barometric Pressure(hPa)'
        ),
        xaxis=dict(
            title='Distribution'
        ),
        title='IQR of Pressure'
    )

    # Trend Analysis
    trend_fig = go.Figure()
    for metric in ['Water Level (m)', 'Temperature (°C)', 'Pressure (Pa)']:
        trend_fig.add_trace(go.Scatter(
            x=df['Timestamp'],
            y=df[metric],
            mode='lines+markers',
            name=metric
        ))

    trend_fig.update_layout(
        yaxis=dict(
            title='Value'
        ),
        xaxis=dict(
            title='Timestamp',
            tickformat='%Y-%m-%d %H:%M:%S',
            tickangle=-45
        ),
        title='Trend Analysis'
    )

    return bar_fig, temp_fig, pressure_fig, z_score_fig_water_level, iqr_fig_water_level, z_score_fig_temp, iqr_fig_temp, z_score_fig_pressure, iqr_fig_pressure, trend_fig

@app.route('/')
def index():
    metric = request.args.get('metric', 'water-level')  # Default to water-level if not specified
    table_name = request.args.get('dwlr', 'dwlr1')  # Default to 'dwlr1' if not specified

    # Extract date parameters
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # Fetch data and alerts
    df, water_level_alerts, battery_level_alerts = fetch_paginated_data_and_alerts(
        50, 0, table_name, start_date, end_date
    )
    bar_fig, temp_fig, pressure_fig, z_score_fig_water_level, iqr_fig_water_level, z_score_fig_temp, iqr_fig_temp, z_score_fig_pressure, iqr_fig_pressure, trend_fig = create_figures_with_hover(
        df)

    # Select the appropriate figure based on the metric
    selected_graph_html = {
        'water-level': bar_fig.to_html(full_html=False),
        'temperature': temp_fig.to_html(full_html=False),
        'pressure': pressure_fig.to_html(full_html=False),
        'trend-analysis': trend_fig.to_html(full_html=False),
    }.get(metric, bar_fig.to_html(full_html=False))

    # Select the appropriate Z-Score and IQR figures based on the metric
    selected_z_score_html = {
        'water-level': z_score_fig_water_level.to_html(full_html=False),
        'temperature': z_score_fig_temp.to_html(full_html=False),
        'pressure': z_score_fig_pressure.to_html(full_html=False),
    }.get(metric, z_score_fig_water_level.to_html(full_html=False))

    selected_iqr_html = {
        'water-level': iqr_fig_water_level.to_html(full_html=False),
        'temperature': iqr_fig_temp.to_html(full_html=False),
        'pressure': iqr_fig_pressure.to_html(full_html=False),
    }.get(metric, iqr_fig_water_level.to_html(full_html=False))

    return render_template(
        'index.html',
        water_level_graph_html=bar_fig.to_html(full_html=False),
        temperature_graph_html=temp_fig.to_html(full_html=False),
        pressure_graph_html=pressure_fig.to_html(full_html=False),
        z_score_graph_html=selected_z_score_html,
        iqr_graph_html=selected_iqr_html,
        trend_graph_html=trend_fig.to_html(full_html=False),
        selected_graph_html=selected_graph_html,
        water_level_alerts=water_level_alerts,
        battery_level_alerts=battery_level_alerts,
        selected_metric=metric,
        start_date=start_date,
        end_date=end_date
    )

if __name__ == '__main__':
    # Start the alert monitoring in a separate thread
    alert_thread = threading.Thread(target=monitor_alerts, daemon=True)
    alert_thread.start()

    app.run(debug=True)
