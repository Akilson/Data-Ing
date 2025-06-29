# IoT Analytics Service

This service analyzes IoT data stored in Minio S3 and creates beautiful visualizations in Grafana.

## 4 Key Analytics Aspects

### 1. Device Health Monitoring
**Question:** Which devices are operating outside optimal conditions?
- Analyzes temperature variance, humidity variance, and tank levels
- Calculates health scores (0-1) based on operational parameters
- Categorizes devices as HEALTHY, WARNING, or CRITICAL

### 2. Geographic Distribution Analysis
**Question:** What's the temperature and humidity distribution across different locations?
- Groups devices by geographic regions (0.1° lat/lon buckets)
- Shows temperature and humidity patterns by location
- Identifies regional environmental variations

### 3. Tank Level Analytics
**Question:** Which tanks need immediate attention and what's the consumption pattern?
- Calculates consumption rates based on historical data
- Estimates days remaining until refill needed
- Prioritizes refill requirements: URGENT, HIGH, MEDIUM, LOW

### 4. Temporal Trends Analysis
**Question:** How do environmental conditions change over time?
- Analyzes hourly patterns of temperature, humidity, and tank levels
- Shows daily cycles and operational patterns
- Helps identify optimal operational windows

## Architecture

```
Minio S3 → Spark Analytics → InfluxDB → Grafana Dashboard
```

1. **Data Source**: IoT events stored in Minio S3 bucket
2. **Processing**: Apache Spark processes and analyzes the data
3. **Storage**: Results stored in InfluxDB for time-series analytics
4. **Visualization**: Grafana displays beautiful, interactive dashboards

## Dashboard Features

- **Real-time Health Status**: Live device health monitoring with color-coded alerts
- **Geographic Heat Map**: Temperature distribution across device locations
- **Tank Level Table**: Sortable table with refill priorities and remaining days
- **Time Series Charts**: 24-hour trends for temperature, humidity, and tank levels

## Usage

1. Start the complete stack:
   ```bash
   docker compose up -d
   ```

2. Access services:
   - Grafana Dashboard: http://localhost:3000 (admin/admin)
   - Minio Console: http://localhost:9001 (minioadmin/minioadmin)
   - InfluxDB: http://localhost:8086

3. The analytics service will automatically:
   - Load data from Minio S3
   - Process and analyze the data
   - Store results in InfluxDB
   - Create/update Grafana dashboards

## Configuration

Environment variables can be customized in `docker-compose.yml`:

- `MINIO_ENDPOINT`: Minio server URL
- `INFLUXDB_URL`: InfluxDB server URL
- `GRAFANA_URL`: Grafana server URL
- `MINIO_BUCKET`: S3 bucket name for IoT events

## Data Schema

Expected IoT event format:
```json
{
  "device_id": "fermenter-001",
  "timestamp": "2025-04-20T12:30:00Z",
  "location": {"lat": 44.8381, "lon": -0.5796},
  "temperature": 27.5,
  "humidity": 60.2,
  "tank_level": 90
}
```
