# EDA Optimising NYC Taxi Operations - Step-by-Step Solution

## 1. Problem Understanding
- Objective used: identify demand, revenue, and operational patterns from 2023 yellow taxi trips.
- Business focus used: routing efficiency, cab positioning, pricing strategy, and customer tipping behavior.
- Data dictionary used from trip records: vendor, pickup/drop datetimes, zones, distance, fare components, payment type, passenger count, and surcharges.

## 2. Data Preparation
- Loaded all 12 monthly parquet files from `data/trip_records`.
- Used stratified sampling by pickup date and pickup hour for each file.
- Sampling fraction used: 5% for each date-hour bucket.
- Combined all sampled monthly data into one dataframe.
- Saved combined sampled data to:
  - `data/trip_records_2023_sampled.parquet`
  - `data/trip_records_2023_sampled.csv`

## 3. Data Cleaning

### 3.1 Column Fixes
- Reset index and removed unnecessary unnamed columns.
- Merged duplicate airport fee columns into one `airport_fee` column.
- Standardized column names to lowercase for consistent analysis.

### 3.2 Negative Monetary Values
- Checked negative values in numeric columns.
- Converted monetary columns and distance to absolute values where needed.

### 3.3 Missing Values
- Calculated missing-value proportion column-wise.
- `passenger_count`: imputed with median of valid positive values; handled zeros similarly.
- `ratecodeid`: imputed with mode.
- `congestion_surcharge`: group-wise median by payment type, then global median fallback.
- Remaining numeric missing values: median imputation.
- Remaining selected categorical missing values: mode imputation.

### 3.4 Outliers
- Created `trip_duration` from pickup/dropoff datetime difference in minutes.
- Removed/handled key problematic records:
  - very low distance but very high fare
  - zero fare and zero distance with different pickup/drop zones
  - extremely large trip distances (>250 miles)
  - invalid `payment_type == 0` replaced with valid mode
  - non-positive or unrealistically long durations (>240 min)
- Removed `passenger_count > 6` as sparse extreme cases.

## 4. General EDA (Patterns and Trends)

### 4.1 Variable Handling
- Used cleaned dataframe columns (lowercase naming).
- Created time features:
  - `pickup_hour`
  - `pickup_day`
  - `pickup_month`

### 4.2 Temporal Trends
- Hourly pickups analyzed and visualized.
- Day-of-week pickup trends analyzed and visualized.
- Month-wise pickup trends analyzed and visualized.

### 4.3 Financial Trends
- Verified zero/negative presence in distance/fare/tip/total.
- Built non-zero analysis frame `df_nz` for stable financial ratio analysis.
- Monthly revenue trend from `total_amount` analyzed.
- Quarterly contribution to annual revenue computed and visualized.

### 4.4 Distance, Fare, Duration, Passenger, Tips
- Correlation and visual relationship computed for:
  - `trip_distance` vs `fare_amount`
  - `trip_duration` vs `fare_amount`
  - `passenger_count` vs `fare_amount`
  - `trip_distance` vs `tip_amount`

### 4.5 Payment Type Distribution
- Mapped payment codes to labels.
- Computed and visualized trip share by payment type.

### 4.6 Geographical Analysis
- Loaded taxi zones shapefile from `data/taxi_zones/taxi_zones.shp` using GeoPandas.
- Merged zone details with trip records using pickup location ID.
- Calculated total trips by zone.
- Added trip counts to zones GeoDataFrame.
- Plotted zone-wise trip density map.
- Displayed top high-traffic zones by trip count.

## 5. Detailed EDA (Insights and Strategies)

### 5.1 Operational Efficiency
- Built route-hour metrics:
  - average distance
  - average duration
  - computed average speed (mph)
- Identified slowest high-volume routes.
- Computed hourly trip counts and busiest hour.
- Scaled sampled hourly counts to actual estimates using 5% sampling factor.
- Compared weekday vs weekend hourly traffic.
- Identified top pickup and dropoff zones and their hourly trends.
- Computed pickup/dropoff ratio per zone to find imbalance extremes.
- Identified top night-hour zones (11 PM to 5 AM) for pickups and dropoffs.
- Computed revenue share for night vs daytime.

### 5.2 Pricing Strategy
- Computed `fare_per_mile`.
- Computed fare-per-mile-per-passenger by passenger count.
- Compared fare-per-mile by hour and by weekday.
- Compared vendor-wise fare-per-mile by hour.
- Distance-tier pricing comparison by vendor for:
  - <=2 miles
  - 2 to 5 miles
  - >5 miles

### 5.3 Customer Experience
- Computed `tip_pct` from tip vs fare.
- Compared tip percentage by:
  - distance tier
  - passenger count
  - pickup hour
- Optional contrast done for low-tip vs high-tip trip groups.
- Analyzed passenger-count variation by hour/day heatmap and by zone.
- Analyzed surcharge prevalence by surcharge type, hour, and high-rate zones.

## 6. Final Recommendation Logic Added in Notebook
- Routing and dispatch recommendations based on demand peaks and slow routes.
- Zone positioning recommendations based on top pickup/drop and night demand clusters.
- Pricing recommendations based on vendor-hour and distance-tier fare behavior, plus tip insights.

## 7. Notes on Approach
- Notebook flow was kept in the same section order as assignment instructions.
- Existing headings/questions were not modified.
- Code was added in required cells only and aligned with the data dictionary fields.
