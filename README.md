# Camera Detection Data Generation Project
This project generates synthetic datasets for testing a Spark RDD-based application that identifies the top items detected by video cameras across different geographical locations. 

The data simulates camera detection events with realistic distributions and includes deliberately introduced data quality issues (duplications) to test the application's robustness.

A sample dataset along with generation logs is available in the [./generate_sample_data/output](./generate_sample_data/output) directory. This can be used as a reference or for quick testing.

## Development Notes
This project was developed as part of an assignment with time constraints. As a result, some best practices were omitted:

- No automated CI/CD pipeline
- No containerization (Docker)
- Limited automated testing (manual verification via comparison.py was performed)

These limitations could be addressed in future iterations to improve the project's robustness and maintainability.


## Objective
The primary objective is to generate realistic test data that:
1. Simulates camera detection events across multiple geographical locations
2. Introduces controlled duplications to test deduplication logic
3. Creates data skew to test optimization capabilities
4. Provides known ground truth for validation
5. Demonstrates the impact of counting vs. not counting duplicates

## Generated Files Summary

| Filename | Description | Purpose |
|----------|-------------|---------|
| camera_detection_events.parquet | Dataset A with duplications | Main input for the application |
| geographical_locations.parquet | Dataset B with location data | Reference data for the application |
| pre_duplication_rankings.parquet | Rankings before any duplications | Baseline reference |
| post_duplication_rankings_duplications_counted.parquet | Rankings after duplication (counting each row) | Shows effect of counting duplicates |
| post_duplication_rankings_duplications_not_counted.parquet | Rankings after duplication (counting each detection_oid once) | Expected output of correct deduplication |
| human_readable_rankings.parquet | Human-readable rankings with location names | Requested data |

## Dataset Specifications

### Dataset A: Camera Detection Events
**File Format**: Parquet  
**Approximate Size**: 1,000,000 rows  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location_oid | bigint | Unique identifier for the geographical location |
| video_camera_oid | bigint | Unique identifier for the video camera |
| detection_oid | bigint | Unique identifier for the detection event |
| item_name | string | Name of the detected item |
| timestamp_detected | bigint | Unix timestamp of detection event |

### Dataset B: Geographical Locations
**File Format**: Parquet  
**Size**: 10,000 rows  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location_oid | bigint | Unique identifier for the geographical location |
| geographical_location | string | Name of the geographical location |

### Output Dataset 1: Pre-Duplication Rankings
**File Format**: Parquet  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location_oid | bigint | Unique identifier for the geographical location |
| item_rank | varchar(500) | Item rank (1 = most popular item in that location) |
| item_name | varchar(5000) | Name of the detected item |

### Output Dataset 2: Post-Duplication Rankings (Duplications Counted)
**File Format**: Parquet  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location_oid | bigint | Unique identifier for the geographical location |
| item_rank | varchar(500) | Item rank when counting all rows including duplicates (1 = most popular) |
| item_name | varchar(5000) | Name of the detected item |

### Output Dataset 3: Post-Duplication Rankings (Duplications Not Counted)
**File Format**: Parquet  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location_oid | bigint | Unique identifier for the geographical location |
| item_rank | varchar(500) | Item rank after deduplication (1 = most popular) |
| item_name | varchar(5000) | Name of the detected item |

### Output Dataset 4: Human-Readable Rankings
**File Format**: Parquet  
**Schema**:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| geographical_location | string | Human-readable geographical location name |
| item_rank | varchar(500) | Item rank after deduplication (1 = most popular) |
| item_name | varchar(5000) | Name of the detected item |

## Data Relationships Assumptions

- **Location to Camera Relationship**: Each `video_camera_oid` belongs to exactly one `geographical_location_oid` (many-to-one relationship)
- **Dataset Linkage**: Each `geographical_location_oid` in Dataset A corresponds to one entry in Dataset B
- **Detection Events**: A unique `detection_oid` represents a single detection event
- **Duplicate Handling**: As per the project requirements, there are duplicate `detection_oid` values due to simulated ingestion errors
- **Deduplication Requirement**: The application should count each `detection_oid` only once, even if it appears multiple times in the dataset

## Duplication Strategy

1. **Universal Duplication**: Duplications occur across all geographical locations
2. **Rank-Preserving Duplications**: In most locations, the most common item is duplicated, which doesn't change the rankings
3. **Rank-Changing Duplications**: In locations 10 and 20, the second most common item is duplicated enough to make it rank first when duplicates are counted

### Duplication Impact Verification

The duplication strategy specifically ensures:

1. For locations 10 and 20:
   - Before duplication: Item A is ranked #1, Item B is ranked #2
   - After duplication (counting duplicates): Item B becomes ranked #1, Item A drops to #2
   - After duplication (not counting duplicates): Item A remains #1, Item B remains #2

2. For all other locations:
   - Before duplication: Item A is ranked #1
   - After duplication (counting duplicates): Item A remains #1
   - After duplication (not counting duplicates): Item A remains #1

## Data Generation Parameters

### Dataset A Parameters:
- **Geographical Locations**: 50 unique locations (with IDs 1-50)
- **Cameras per Location**: 5-20 cameras (varies by location, assigned sequential IDs)
- **Detectable Items**: 100 different realistic items (e.g., "person", "car", "dog", "bicycle")
- **Time Range**: Sequential timestamps (each increment in detection_oid corresponds to 1 second increase)
- **Item Distribution**: Follows a power law distribution (some items are much more common than others)
- **Data Skew**: Location ID 42 has 5x the average number of detections

### Dataset B Parameters:
- **Location Types**: Urban areas, suburbs, rural locations, commercial districts, etc.
- **Naming Convention**: "{Type} {Number}" (e.g., "Urban Area 7", "Commercial District 12")
- **Note**: Only 10,000 rows are generated for Dataset B, with the first 50 being used in Dataset A

## Special Case: Data Skew
To test the optimization requirement for handling data skew, the location with ID 42 will have a significantly higher number of detections than other locations (approximately 5x the average). This represents a high-traffic area with many more detection events.

## Assumptions Made

In generating this sample data, the following assumptions have been made:

1. **Sequential IDs**: Location IDs and camera IDs are assigned in ascending order for simplicity
2. **Camera Distribution**: Each location has 5-20 cameras, with the exact number randomly determined
3. **Power Law Distribution**: Item frequency follows a realistic power law distribution where common items appear much more frequently than rare items
4. **Temporal Consistency**: Detection_oid values increase sequentially and have a strict linear relationship with timestamp_detected

## Expected Outputs

The data generation process produces six files:

1. **Dataset A**: Camera detection events with duplications (`camera_detection_events.parquet`)
2. **Dataset B**: Geographical location reference data (`geographical_locations.parquet`)
3. **Pre-Duplication Rankings**: Rankings before any duplications are added (`pre_duplication_rankings.parquet`)
4. **Post-Duplication Rankings (Duplications Counted)**: Rankings after duplications are added, with each row counted including duplicates (`post_duplication_rankings_duplications_counted.parquet`)
5. **Post-Duplication Rankings (Duplications Not Counted)**: Rankings after duplications are added, with each detection_oid counted only once (`post_duplication_rankings_duplications_not_counted.parquet`)
6. **Human-Readable Rankings**: Rankings with human-readable location names instead of IDs (`human_readable_rankings.parquet`)

These files provide both the test inputs and comprehensive validation references for the Spark application.

### Interpretation of Output Files

- In locations 10 and 20, the rankings in file #4 should differ from the rankings in file #5
- File #5 should match the rankings in file #3 for all locations, confirming proper deduplication
- For all other locations, all ranking files should show consistent rankings regardless of duplication handling
- File #6 provides the same data as file #5 but uses human-readable location names instead of IDs


## Configuration

Data generation parameters can be customized through the `config.yaml` file instead of modifying the source code. All parameters are documented in the config file with explanations of their purpose and effects.

### Configuration Parameters

#### General Settings
- `seed`: Random seed for reproducibility. Using the same seed will generate identical data.
- `output_dir`: Directory where all output files will be created.

#### Dataset Size Parameters
- `top_x_items`: Number of top items to include in the ranking outputs. Higher values provide more complete rankings but increase file size.
- `num_locations`: Number of geographical locations to use in Dataset A. These locations will have cameras and detection events.
- `num_dataset_b_rows`: Total number of rows in Dataset B (geographical locations reference table). Only the first `num_locations` will be used in Dataset A.
- `num_items`: Number of unique detectable items in the dataset. These represent objects that can be detected by cameras (car, person, etc.).
- `num_detection_events`: Number of detection events to generate in Dataset A. This is the main parameter controlling dataset size.

#### Data Quality Parameters
- `rank_changing_locations`: Locations where duplication affects ranking (changes which item is #1). For these locations, the second most common item will be duplicated enough to make it rank first when duplicates are counted.
- `data_skew_location`: Location with skewed data distribution. This location will have more detection events than others.
- `data_skew_factor`: Factor by which the skewed location has more detections. A factor of 5 means the skewed location has 5x more events than average.
- `reference_date`: Fixed date for timestamp generation in format "YYYY-MM-DD". This ensures consistent timestamps regardless of when the script is run.

## Example Configuration

```yaml
# General Settings
seed: 42
output_dir: "output"

# Dataset Size Parameters
top_x_items: 10
num_locations: 50
num_dataset_b_rows: 10000
num_items: 100
num_detection_events: 1000000

# Data Quality Parameters
rank_changing_locations: [10, 20]
data_skew_location: 42
data_skew_factor: 5
reference_date: "2025-01-29"
```

### Modifying the Configuration

To modify the data generation parameters:
1. Edit the `config.yaml` file
2. Run the data generation script
3. New data will be generated according to your specifications

No changes to the source code are required when adjusting these parameters.

## Logging

The data generation process produces detailed logs that document:
- Configuration parameters used for generation
- Statistics about the generated data
- Verification of duplication effects on rankings
- Confirmation that the duplication strategy worked as intended

The log file is saved to `output/data_generation.log` for reference.