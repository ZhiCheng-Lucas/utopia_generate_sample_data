"""
Simple script for sanity test to check if two Parquet files are equivalent.
"""

import pandas as pd

# FILE1_PATH = "./output/post_duplication_rankings_duplications_not_counted.parquet"
# FILE2_PATH = (
#     "./output2/part-00000-4b0c8726-c030-41a7-a383-e1f05ed98198-c000.snappy.parquet"
# )

# # Should not match
# FILE1_PATH = "./output/post_duplication_rankings_duplications_counted.parquet"
# FILE2_PATH = "./output/pre_duplication_rankings.parquet"

# # Should match
# FILE1_PATH = "./output/post_duplication_rankings_duplications_not_counted.parquet"
# FILE2_PATH = "./output/pre_duplication_rankings.parquet"


FILE1_PATH = "./generate_sample_data/output/human_readable_rankings.parquet"
FILE2_PATH = "./generate_sample_data/output2/part-00000-81676415-8b47-4950-bbe1-020326473b46-c000.snappy.parquet"


print(f"Reading file 1: {FILE1_PATH}")
df1 = pd.read_parquet(FILE1_PATH)

print(f"Reading file 2: {FILE2_PATH}")
df2 = pd.read_parquet(FILE2_PATH)

# Normalize types in both dataframes
df1["geographical_location"] = df1["geographical_location"].astype(str)
df1["item_rank"] = df1["item_rank"].astype(str)
df1["item_name"] = df1["item_name"].astype(str)

df2["geographical_location"] = df2["geographical_location"].astype(str)
df2["item_rank"] = df2["item_rank"].astype(str)
df2["item_name"] = df2["item_name"].astype(str)

# Sort both dataframes by geographical_location_oid and item_rank
df1_sorted = df1.sort_values(by=["geographical_location", "item_rank"]).reset_index(
    drop=True
)
df2_sorted = df2.sort_values(by=["geographical_location", "item_rank"]).reset_index(
    drop=True
)

# Check if dataframes are equal
are_equal = df1_sorted.equals(df2_sorted)

if are_equal:
    print("PASS: Files are equivalent after sorting and type conversion")
else:
    print("FAIL: Files are not equivalent")
    # Print first few differing rows if not equal
    for i in range(min(len(df1_sorted), len(df2_sorted), 5)):
        if not df1_sorted.iloc[i].equals(df2_sorted.iloc[i]):
            print(f"Row {i} differs:")
            print(f"  File 1: {df1_sorted.iloc[i].to_dict()}")
            print(f"  File 2: {df2_sorted.iloc[i].to_dict()}")
