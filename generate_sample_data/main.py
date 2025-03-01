"""
Camera Detection Data Generator

This script generates synthetic datasets for testing a Spark RDD-based application
that identifies top items detected by video cameras across different geographical locations.
"""

import os
import random

from datetime import datetime
import time
import logging

import numpy as np
import pandas as pd
import yaml
import pyarrow as pa
import pyarrow.parquet as pq


def load_config():
    """Load configuration from YAML file"""
    # Get the directory of the current script
    script_dir = os.path.dirname(__file__)
    config_path = os.path.join(script_dir, "config.yaml")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Load configuration
config = load_config()

# Configuration constants
# Get the directory of the current script for the output directory
script_dir = os.path.dirname(__file__)
OUTPUT_DIR = os.path.join(script_dir, config["output_dir"])

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging
LOG_FILE = os.path.join(OUTPUT_DIR, "data_generation.log")

# Configure logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration constants (except OUTPUT_DIR which is defined above)
SEED = config["seed"]  # For reproducibility
TOP_X_ITEMS = config["top_x_items"]  # Number of top items to include in rankings
NUM_LOCATIONS = config["num_locations"]
NUM_DATASET_B_ROWS = config["num_dataset_b_rows"]
NUM_ITEMS = config["num_items"]
NUM_DETECTION_EVENTS = config["num_detection_events"]
RANK_CHANGING_LOCATIONS = config[
    "rank_changing_locations"
]  # Locations where duplication affects ranking
DATA_SKEW_LOCATION = config["data_skew_location"]
DATA_SKEW_FACTOR = config["data_skew_factor"]  # Location 42 has 5x average detections
# Add this with the other constants
REFERENCE_DATE = config["reference_date"]
# Convert string date to timestamp
REFERENCE_TIMESTAMP = int(datetime.strptime(REFERENCE_DATE, "%Y-%m-%d").timestamp())

# Set random seed for reproducibility
random.seed(SEED)
np.random.seed(SEED)


def generate_location_types():
    """Generate realistic location types."""
    return [
        "Urban Area",
        "Suburban Area",
        "Rural Area",
        "Commercial District",
        "Industrial Zone",
        "Residential Area",
        "Downtown",
        "Shopping Center",
        "Business Park",
        "Transit Hub",
        "Educational Campus",
        "Public Park",
        "Cultural District",
        "Entertainment District",
        "Medical Complex",
        "Government Complex",
        "Airport Area",
        "Waterfront",
        "Highway Junction",
        "Historic District",
    ]


def generate_detectable_items():
    """Generate a list of 100 realistic detectable items with frequency weights."""
    items = [
        "person",
        "car",
        "truck",
        "bicycle",
        "motorcycle",
        "bus",
        "dog",
        "cat",
        "backpack",
        "umbrella",
        "handbag",
        "tie",
        "suitcase",
        "frisbee",
        "skis",
        "snowboard",
        "sports ball",
        "kite",
        "baseball bat",
        "baseball glove",
        "skateboard",
        "surfboard",
        "tennis racket",
        "bottle",
        "wine glass",
        "cup",
        "fork",
        "knife",
        "spoon",
        "bowl",
        "banana",
        "apple",
        "sandwich",
        "orange",
        "broccoli",
        "carrot",
        "hot dog",
        "pizza",
        "donut",
        "cake",
        "chair",
        "couch",
        "potted plant",
        "bed",
        "dining table",
        "toilet",
        "tv",
        "laptop",
        "mouse",
        "remote",
        "keyboard",
        "cell phone",
        "microwave",
        "oven",
        "toaster",
        "sink",
        "refrigerator",
        "book",
        "clock",
        "vase",
        "scissors",
        "teddy bear",
        "hair drier",
        "toothbrush",
        "traffic light",
        "fire hydrant",
        "stop sign",
        "parking meter",
        "bench",
        "bird",
        "horse",
        "sheep",
        "cow",
        "elephant",
        "bear",
        "zebra",
        "giraffe",
        "hat",
        "shoe",
        "eye glasses",
        "scooter",
        "skateboarding",
        "baby stroller",
        "shopping cart",
        "basketball",
        "volleyball",
        "soccer ball",
        "baseball",
        "tennis ball",
        "golf ball",
        "hockey stick",
        "cricket bat",
        "luggage",
        "trash bin",
        "mailbox",
        "street light",
        "street sign",
        "construction barrier",
        "briefcase",
        "toolbox",
    ]

    # Ensure we have at least 100 items
    while len(items) < NUM_ITEMS:
        items.append(f"object_{len(items)}")

    # Limit to exactly NUM_ITEMS
    return items[:NUM_ITEMS]


def generate_power_law_weights(n, alpha=1.5):
    """
    Generate weights following a power law distribution.

    Args:
        n: Number of weights to generate
        alpha: Parameter controlling distribution shape (higher = more skewed)

    Returns:
        Array of weights that follow a power law distribution
    """
    # Generate power law distribution (rank^(-alpha))
    ranks = np.arange(1, n + 1)
    weights = ranks ** (-alpha)

    # Normalize to make probabilities sum to 1
    return weights / weights.sum()


def generate_dataset_b():
    """
    Generate Dataset B: Geographical Locations

    Returns:
        DataFrame containing geographical location data
    """
    logger.info("Generating Dataset B: Geographical Locations")

    location_types = generate_location_types()

    # Generate 10,000 rows for Dataset B
    data = []

    # First, generate the 50 locations that will be used in Dataset A
    for location_id in range(1, NUM_LOCATIONS + 1):
        location_type = random.choice(location_types)
        location_number = random.randint(1, 100)
        location_name = f"{location_type} {location_number}"
        data.append(
            {
                "geographical_location_oid": location_id,
                "geographical_location": location_name,
            }
        )

    # Generate the remaining locations for Dataset B
    for location_id in range(NUM_LOCATIONS + 1, NUM_DATASET_B_ROWS + 1):
        location_type = random.choice(location_types)
        location_number = random.randint(1, 100)
        location_name = f"{location_type} {location_number}"
        data.append(
            {
                "geographical_location_oid": location_id,
                "geographical_location": location_name,
            }
        )

    return pd.DataFrame(data)


def generate_camera_assignments():
    """
    Generate camera assignments for each geographical location.

    Returns:
        Dictionary mapping location_id to list of camera_ids
    """
    camera_assignments = {}
    next_camera_id = 1

    for location_id in range(1, NUM_LOCATIONS + 1):
        # Assign 5-20 cameras per location
        num_cameras = random.randint(5, 20)
        camera_ids = list(range(next_camera_id, next_camera_id + num_cameras))
        camera_assignments[location_id] = camera_ids
        next_camera_id += num_cameras

    return camera_assignments


def generate_dataset_a_no_duplicates(camera_assignments):
    """
    Generate Dataset A: Camera Detection Events without duplications

    Args:
        camera_assignments: Dictionary mapping location_id to list of camera_ids

    Returns:
        DataFrame containing camera detection events without duplications
    """
    logger.info("Generating Dataset A: Camera Detection Events (without duplicates)")

    # Generate list of detectable items with power law weights
    items = generate_detectable_items()
    item_weights = generate_power_law_weights(len(items))

    # Calculate approx detections per location (accounting for data skew)
    base_detections_per_location = NUM_DETECTION_EVENTS / (
        NUM_LOCATIONS + (DATA_SKEW_FACTOR - 1)  # Adjusting for the skewed location
    )

    # Start timestamp
    start_timestamp = REFERENCE_TIMESTAMP

    data = []
    detection_oid = 1
    current_timestamp = start_timestamp

    # Generate detection events for each location
    for location_id in range(1, NUM_LOCATIONS + 1):
        cameras = camera_assignments[location_id]

        # Calculate how many detections for this location
        if location_id == DATA_SKEW_LOCATION:
            num_detections = int(base_detections_per_location * DATA_SKEW_FACTOR)
        else:
            num_detections = int(base_detections_per_location)

        for _ in range(num_detections):
            camera_id = random.choice(cameras)
            item = np.random.choice(items, p=item_weights)

            data.append(
                {
                    "geographical_location_oid": location_id,
                    "video_camera_oid": camera_id,
                    "detection_oid": detection_oid,
                    "item_name": item,
                    "timestamp_detected": current_timestamp,
                }
            )

            detection_oid += 1
            current_timestamp += 1  # Increment timestamp by 1 second

    # Convert to DataFrame and shuffle rows
    df = pd.DataFrame(data)
    df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

    return df


def calculate_rankings(df, count_duplicates=False):
    """
    Calculate item rankings for each geographical location.

    Args:
        df: DataFrame containing detection events
        count_duplicates: Whether to count duplicate detection_oid values

    Returns:
        DataFrame with rankings per location
    """
    counting_method = (
        "counting duplicates" if count_duplicates else "ignoring duplicates"
    )
    logger.info("Calculating rankings (%s)", counting_method)

    # If not counting duplicates, remove duplicates based on detection_oid
    if not count_duplicates:
        df = df.drop_duplicates(subset=["detection_oid"])

    # Group by location and item, count occurrences
    grouped = (
        df.groupby(["geographical_location_oid", "item_name"])
        .size()
        .reset_index(name="count")
    )

    # Prepare rankings data
    rankings_data = []

    # For each location, determine rankings
    for location_id in range(1, NUM_LOCATIONS + 1):
        location_items = grouped[grouped["geographical_location_oid"] == location_id]

        # Sort by count (descending)
        location_items = location_items.sort_values("count", ascending=False)

        # Add ranking information
        for i, (_, row) in enumerate(location_items.iterrows(), 1):
            if i <= TOP_X_ITEMS:  # Only include top X items
                rankings_data.append(
                    {
                        "geographical_location_oid": location_id,
                        "item_rank": str(
                            i
                        ),  # Using string as specified in requirements
                        "item_name": row["item_name"],
                        "item_count": row["count"],  # Include count for verification
                    }
                )

    return pd.DataFrame(rankings_data)


def add_duplications(df):
    """
    Add duplications to the dataset according to the specified rules:
    - For locations 10 and 20: Duplicate the second most common item to make it rank first
    - For all other locations: Duplicate the most common item (doesn't change ranking)

    Args:
        df: DataFrame containing detection events without duplications

    Returns:
        DataFrame with duplications added
    """
    logger.info("Adding duplications to Dataset A")

    # Make a copy of the original dataframe
    df_with_duplicates = df.copy()

    # For each location, add duplications
    for location_id in range(1, NUM_LOCATIONS + 1):
        location_df = df[df["geographical_location_oid"] == location_id]

        # Count original items to determine which to duplicate
        item_counts = location_df["item_name"].value_counts()

        if len(item_counts) < 2:
            logger.warning(
                "Location %d has fewer than 2 unique items, skipping duplication",
                location_id,
            )
            continue

        # Get top two items
        top_items = item_counts.index.tolist()[:2]
        most_common = top_items[0]
        second_most_common = top_items[1]

        most_common_count = item_counts[most_common]
        second_most_count = item_counts[second_most_common]

        logger.info("Location %d - Before duplication:", location_id)
        logger.info(
            "  Most common: '%s' (%d occurrences)", most_common, most_common_count
        )
        logger.info(
            "  Second most: '%s' (%d occurrences)",
            second_most_common,
            second_most_count,
        )

        # Determine which item to duplicate and how many times
        if location_id in RANK_CHANGING_LOCATIONS:
            # For special locations, duplicate the second most common item
            item_to_duplicate = second_most_common

            # Calculate how many duplications needed to outrank the most common item
            # Add a significant buffer to ensure it outranks
            duplications_needed = most_common_count - second_most_count + 100

            logger.info(
                "  Adding %d duplicates of '%s' to make it outrank '%s'",
                duplications_needed,
                second_most_common,
                most_common,
            )
        else:
            # For other locations, duplicate the most common item
            item_to_duplicate = most_common
            # Just add some duplicates (won't change ranking)
            duplications_needed = random.randint(5, 20)
            logger.info(
                "  Adding %d duplicates of '%s' (won't change ranking)",
                duplications_needed,
                most_common,
            )

        # Find rows to duplicate
        rows_with_target_item = location_df[
            location_df["item_name"] == item_to_duplicate
        ]

        # Make sure we have rows to duplicate
        if len(rows_with_target_item) == 0:
            logger.warning(
                "  No '%s' items found in location %d, skipping duplication",
                item_to_duplicate,
                location_id,
            )
            continue

        # Create duplications
        rows_to_duplicate = []
        for _ in range(duplications_needed):
            # Pick a random row to duplicate
            random_row = rows_with_target_item.sample(
                n=1, random_state=len(rows_to_duplicate)
            )
            rows_to_duplicate.append(random_row)

        # Combine all duplicated rows
        if rows_to_duplicate:
            duplicated_rows = pd.concat(rows_to_duplicate)
            # Add duplicates to the dataframe
            df_with_duplicates = pd.concat([df_with_duplicates, duplicated_rows])

            # Verify the duplication impact for special locations
            if location_id in RANK_CHANGING_LOCATIONS:
                new_counts = df_with_duplicates[
                    df_with_duplicates["geographical_location_oid"] == location_id
                ]["item_name"].value_counts()
                logger.info("  After duplication (with duplicates counted):")
                logger.info(
                    "    '%s': %d occurrences",
                    second_most_common,
                    new_counts.get(second_most_common, 0),
                )
                logger.info(
                    "    '%s': %d occurrences",
                    most_common,
                    new_counts.get(most_common, 0),
                )

                # Check if our duplication strategy worked
                top_item_with_dupes = new_counts.index[0]
                logger.info(
                    "    Top item with duplicates counted: '%s'", top_item_with_dupes
                )
                if top_item_with_dupes != second_most_common:
                    logger.warning(
                        "    Duplication strategy failed! '%s' should be top item.",
                        second_most_common,
                    )

    return df_with_duplicates.reset_index(drop=True)


def create_human_readable_rankings(rankings_df, locations_df):
    """
    Creates a human-readable rankings dataset by joining the rankings with location names.

    Args:
        rankings_df: DataFrame containing post-duplication rankings (duplications not counted)
        locations_df: DataFrame containing geographical location data (Dataset B)

    Returns:
        DataFrame with human-readable location names instead of IDs
    """
    logger.info("Generating human-readable rankings dataset")

    # Join the rankings with the locations dataframe to get the location names
    human_readable_df = pd.merge(
        rankings_df, locations_df, on="geographical_location_oid", how="left"
    )

    # Select only the required columns and rename them
    human_readable_df = human_readable_df[
        ["geographical_location", "item_rank", "item_name"]
    ]

    # Ensure the schema is correct
    human_readable_df["geographical_location"] = human_readable_df[
        "geographical_location"
    ].astype("string")
    human_readable_df["item_rank"] = human_readable_df["item_rank"].astype("string")
    human_readable_df["item_name"] = human_readable_df["item_name"].astype("string")

    return human_readable_df


def save_human_readable_rankings(df, filename):
    """
    Save human-readable rankings DataFrame as Parquet file.

    Args:
        df: DataFrame to save
        filename: Output filename

    Returns:
        file_path: Path where the file was saved
    """
    # Define schema for human-readable rankings
    schema = pa.schema(
        [
            ("geographical_location", pa.string()),
            ("item_rank", pa.string()),
            ("item_name", pa.string()),
        ]
    )

    # Convert pandas DataFrame to PyArrow Table with schema
    table = pa.Table.from_pandas(df, schema=schema)

    file_path = os.path.join(OUTPUT_DIR, filename)
    pq.write_table(table, file_path)

    logger.info("Saved %d rows to %s", len(df), file_path)
    return file_path


def ensure_data_types(df, file_type):
    """
    Ensure the DataFrame has the correct column data types based on file type.

    Args:
        df: DataFrame to modify
        file_type: String indicating which file/schema to apply

    Returns:
        DataFrame with correct data types
    """
    df_copy = df.copy()

    if file_type == "dataset_a":
        # For camera_detection_events.parquet
        df_copy["geographical_location_oid"] = df_copy[
            "geographical_location_oid"
        ].astype("int64")
        df_copy["video_camera_oid"] = df_copy["video_camera_oid"].astype("int64")
        df_copy["detection_oid"] = df_copy["detection_oid"].astype("int64")
        df_copy["item_name"] = df_copy["item_name"].astype("string")
        df_copy["timestamp_detected"] = df_copy["timestamp_detected"].astype("int64")

    elif file_type == "dataset_b":
        # For geographical_locations.parquet
        df_copy["geographical_location_oid"] = df_copy[
            "geographical_location_oid"
        ].astype("int64")
        df_copy["geographical_location"] = df_copy["geographical_location"].astype(
            "string"
        )

    elif file_type == "rankings":
        # For all ranking files
        df_copy["geographical_location_oid"] = df_copy[
            "geographical_location_oid"
        ].astype("int64")
        df_copy["item_rank"] = df_copy["item_rank"].astype("string")
        df_copy["item_name"] = df_copy["item_name"].astype("string")
        # Keep item_count as is if it exists, it will be dropped later

    return df_copy


def save_dataframe_as_parquet(df, filename, file_type):
    """
    Save DataFrame as Parquet file with correct data types.

    Args:
        df: DataFrame to save
        filename: Output filename
        file_type: String indicating which file/schema to apply

    Returns:
        file_path: Path where the file was saved
    """
    # Apply correct data types
    df_typed = ensure_data_types(df, file_type)

    # Define schema based on file type
    if file_type == "dataset_a":
        schema = pa.schema(
            [
                ("geographical_location_oid", pa.int64()),
                ("video_camera_oid", pa.int64()),
                ("detection_oid", pa.int64()),
                ("item_name", pa.string()),
                ("timestamp_detected", pa.int64()),
            ]
        )
    elif file_type == "dataset_b":
        schema = pa.schema(
            [
                ("geographical_location_oid", pa.int64()),
                ("geographical_location", pa.string()),
            ]
        )
    elif file_type == "rankings":
        # Check if item_count is in the DataFrame
        if "item_count" in df_typed.columns:
            # Will be saved but dropped later for final output
            schema = pa.schema(
                [
                    ("geographical_location_oid", pa.int64()),
                    ("item_rank", pa.string()),
                    ("item_name", pa.string()),
                    ("item_count", pa.int64()),
                ]
            )
        else:
            schema = pa.schema(
                [
                    ("geographical_location_oid", pa.int64()),
                    ("item_rank", pa.string()),
                    ("item_name", pa.string()),
                ]
            )
    else:
        raise ValueError(
            f"Unknown file_type: {file_type}. Expected 'dataset_a', 'dataset_b', or 'rankings'."
        )
    # Convert pandas DataFrame to PyArrow Table with schema
    table = pa.Table.from_pandas(df_typed, schema=schema)

    file_path = os.path.join(OUTPUT_DIR, filename)
    pq.write_table(table, file_path)

    logger.info("Saved %d rows to %s", len(df), file_path)
    return file_path


def drop_item_count_column(df):
    """Remove the item_count column if it exists."""
    if "item_count" in df.columns:
        return df.drop("item_count", axis=1)
    return df


def main():
    """Main function to generate all datasets."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("CAMERA DETECTION DATA GENERATION PROCESS")
    logger.info("=" * 80)
    logger.info("Configuration:")
    logger.info("  Number of locations: %d", NUM_LOCATIONS)
    logger.info("  Dataset B rows: %d", NUM_DATASET_B_ROWS)
    logger.info("  Number of detectable items: %d", NUM_ITEMS)
    logger.info("  Number of detection events: %d", NUM_DETECTION_EVENTS)
    logger.info("  Rank-changing locations: %s", RANK_CHANGING_LOCATIONS)
    logger.info(
        "  Data skew location: %d (factor: %dx)", DATA_SKEW_LOCATION, DATA_SKEW_FACTOR
    )
    logger.info("  Output directory: %s", OUTPUT_DIR)
    logger.info("  Log file: %s", LOG_FILE)
    logger.info("Starting data generation process")

    # Generate Dataset B: Geographical Locations
    dataset_b = generate_dataset_b()
    save_dataframe_as_parquet(dataset_b, "geographical_locations.parquet", "dataset_b")

    # Generate camera assignments
    camera_assignments = generate_camera_assignments()

    # Generate Dataset A: Camera Detection Events (without duplicates)
    dataset_a_no_duplicates = generate_dataset_a_no_duplicates(camera_assignments)

    # Calculate pre-duplication rankings (there are no duplicates yet, so only one file needed)
    pre_duplication_rankings = calculate_rankings(
        dataset_a_no_duplicates, count_duplicates=True
    )
    save_dataframe_as_parquet(
        drop_item_count_column(pre_duplication_rankings),
        "pre_duplication_rankings.parquet",
        "rankings",
    )

    # Add duplications
    dataset_a_with_duplicates = add_duplications(dataset_a_no_duplicates)
    save_dataframe_as_parquet(
        dataset_a_with_duplicates, "camera_detection_events.parquet", "dataset_a"
    )

    # Calculate post-duplication rankings (two versions)
    # 1. With duplicates counted (counting each row, including duplicates)
    post_duplication_rankings_with_dupes = calculate_rankings(
        dataset_a_with_duplicates, count_duplicates=True
    )
    save_dataframe_as_parquet(
        drop_item_count_column(post_duplication_rankings_with_dupes),
        "post_duplication_rankings_duplications_counted.parquet",
        "rankings",
    )

    # 2. With duplicates not counted (counting each detection_oid only once)
    post_duplication_rankings_no_dupes = calculate_rankings(
        dataset_a_with_duplicates, count_duplicates=False
    )
    save_dataframe_as_parquet(
        drop_item_count_column(post_duplication_rankings_no_dupes),
        "post_duplication_rankings_duplications_not_counted.parquet",
        "rankings",
    )

    # 3. NEW: Generate human-readable rankings (based on post_duplication_rankings_no_dupes)
    human_readable_rankings = create_human_readable_rankings(
        post_duplication_rankings_no_dupes, dataset_b
    )
    save_human_readable_rankings(
        human_readable_rankings, "human_readable_rankings.parquet"
    )

    # Print some statistics
    logger.info("Generated %d geographical locations", len(dataset_b))
    logger.info(
        "Generated %d detection events (including duplicates)",
        len(dataset_a_with_duplicates),
    )
    logger.info(
        "Number of unique detection_oid values: %d",
        dataset_a_with_duplicates["detection_oid"].nunique(),
    )

    # List all output files
    logger.info("Files generated:")
    logger.info("  1. camera_detection_events.parquet (Dataset A)")
    logger.info("  2. geographical_locations.parquet (Dataset B)")
    logger.info("  3. pre_duplication_rankings.parquet")
    logger.info("  4. post_duplication_rankings_duplications_counted.parquet")
    logger.info("  5. post_duplication_rankings_duplications_not_counted.parquet")
    logger.info("  6. human_readable_rankings.parquet")

    # Print verification for rank-changing locations
    for location_id in RANK_CHANGING_LOCATIONS:
        # Get top items from different ranking methods
        pre_top_item = pre_duplication_rankings[
            (pre_duplication_rankings["geographical_location_oid"] == location_id)
            & (pre_duplication_rankings["item_rank"] == "1")
        ]["item_name"].values[0]

        post_top_item_with_dupes = post_duplication_rankings_with_dupes[
            (
                post_duplication_rankings_with_dupes["geographical_location_oid"]
                == location_id
            )
            & (post_duplication_rankings_with_dupes["item_rank"] == "1")
        ]["item_name"].values[0]

        post_top_item_no_dupes = post_duplication_rankings_no_dupes[
            (
                post_duplication_rankings_no_dupes["geographical_location_oid"]
                == location_id
            )
            & (post_duplication_rankings_no_dupes["item_rank"] == "1")
        ]["item_name"].values[0]

        logger.info("Final verification for location %d:", location_id)
        logger.info("  Pre-duplication top item: %s", pre_top_item)
        logger.info(
            "  Post-duplication top item (duplications counted): %s",
            post_top_item_with_dupes,
        )
        logger.info(
            "  Post-duplication top item (duplications not counted): %s",
            post_top_item_no_dupes,
        )

        # Check if our duplication strategy worked
        if post_top_item_with_dupes == post_top_item_no_dupes:
            logger.warning("  FAILED: Duplication did not change ranking as expected!")
        else:
            logger.info("  SUCCESS: Duplication changed ranking as expected")

        # Get counts for verification
        with_dupes_row = post_duplication_rankings_with_dupes[
            (
                post_duplication_rankings_with_dupes["geographical_location_oid"]
                == location_id
            )
            & (post_duplication_rankings_with_dupes["item_rank"] == "1")
        ]
        no_dupes_row = post_duplication_rankings_no_dupes[
            (
                post_duplication_rankings_no_dupes["geographical_location_oid"]
                == location_id
            )
            & (post_duplication_rankings_no_dupes["item_rank"] == "1")
        ]

        if not with_dupes_row.empty and not no_dupes_row.empty:
            logger.info("  Counts:")
            logger.info(
                "    '%s' (with duplications): %d",
                post_top_item_with_dupes,
                with_dupes_row["item_count"].values[0],
            )
            logger.info(
                "    '%s' (without duplications): %d",
                post_top_item_no_dupes,
                no_dupes_row["item_count"].values[0],
            )

    elapsed_time = time.time() - start_time
    logger.info("Data generation completed in %.2f seconds", elapsed_time)
    logger.info("All files have been saved to %s", OUTPUT_DIR)
    logger.info("Log file has been saved to %s", LOG_FILE)
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
