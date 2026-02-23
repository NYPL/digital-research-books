"""
This is ad-hoc exploration to estimate the serialized size of the book
attributes we plan to add to the search index for the VRA.

Takes 1000 of attribute sets, serializes them and introspects the distribution
of sizes.
"""

import sys
import json
import pandas as pd

from load_env import load_env_file

load_env_file("vra", file_string="config/{}.yaml")

from managers import DBManager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from utils.utils import read_env

# Setup database connection
DB_URL = read_env("POSTGRES_READ_HOST")
engine = DBManager(host=DB_URL).generate_engine()
Session = sessionmaker(bind=engine)

# Query to get first 1000 public domain GRIN editions
query = text("""
    SELECT -- 3.872s
        e.id AS edition_id,
        e.publication_date,
        e.languages,
        w.subjects
    FROM (
        -- Get only the IDs we need first
        SELECT DISTINCT i.edition_id
        FROM records r
        JOIN grin_statuses gs ON r.id = gs.record_id
        JOIN items i ON r.id = i.record_id
        WHERE gs.state = 'downloaded'
        AND split_part(r.rights, '|', 2) IN ('public_domain', 'https://creativecommons.org/publicdomain/zero/1.0/')
        LIMIT 1000
    ) AS sub
    JOIN editions e ON sub.edition_id = e.id
    JOIN works w ON e.work_id = w.id;
""")

print("Fetching data from database...")
with Session() as session:
    result = session.execute(query)
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

print(f"Retrieved {len(df)} editions")
print("\nSample data:")
print(df.head())


# Calculate serialized size for each field as JSON-encoded strings
def get_json_string_size(value):
    """Get the size of a value as a JSON-encoded string in kilobytes"""
    if value is None:
        return sys.getsizeof("") / 1024  # Empty string size in KB
    try:
        # Convert to JSON string and get its size in KB
        json_str = json.dumps(value, default=str)  # default=str handles dates
        return sys.getsizeof(json_str) / 1024
    except:
        # Fallback to string representation
        return sys.getsizeof(str(value)) / 1024


print("\nCalculating serialized sizes...")
df["edition_id_size"] = df["edition_id"].apply(get_json_string_size)
df["publication_date_size"] = df["publication_date"].apply(get_json_string_size)
df["languages_size"] = df["languages"].apply(get_json_string_size)
df["subjects_size"] = df["subjects"].apply(get_json_string_size)

# Calculate total size per row
df["total_size"] = (
    df["edition_id_size"]
    + df["publication_date_size"]
    + df["languages_size"]
    + df["subjects_size"]
)

print("\n" + "=" * 60)
print("DISTRIBUTION OF JSON-ENCODED STRING SIZES (kilobytes)")
print("=" * 60)

# Get statistics for each field
print("\nEdition ID Size:")
print(df["edition_id_size"].describe())

print("\nPublication Date Size:")
print(df["publication_date_size"].describe())

print("\nLanguages Size:")
print(df["languages_size"].describe())

print("\nSubjects Size:")
print(df["subjects_size"].describe())

print("\nCombined metadata size per edition:")
print(df["total_size"].describe())

# # Additional statistics
# print("\n" + "="*60)
# print("ADDITIONAL STATISTICS")
# print("="*60)

# print(f"\nTotal disk space for all 1000 editions: {df['total_size'].sum():,.2f} KB ({df['total_size'].sum() / 1024:.2f} MB)")
# print(f"Average size per edition: {df['total_size'].mean():,.2f} KB")
# print(f"Median size per edition: {df['total_size'].median():,.2f} KB")

# Show breakdown by field
field_totals = {
    "edition_id": df["edition_id_size"].sum(),
    "publication_date": df["publication_date_size"].sum(),
    "languages": df["languages_size"].sum(),
    "subjects": df["subjects_size"].sum(),
}

print("\nSize breakdown by field:")
for field, size in sorted(field_totals.items(), key=lambda x: x[1], reverse=True):
    pct = (size / df["total_size"].sum()) * 100
    print(f"  {field:20s}: {size:>10,.2f} KB ({pct:>5.1f}%)")

# Check for null values
print("\nNull value counts:")
print(df[["edition_id", "publication_date", "languages", "subjects"]].isnull().sum())
