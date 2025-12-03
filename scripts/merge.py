import os
import ujson
import pandas as pd
from tqdm import tqdm
from copy import deepcopy

def normalize_qubit_data(data):
    """
    Remove timestamp fields from qubit/gate entries to compare content only.
    """
    normalized = deepcopy(data)
    if isinstance(normalized, list):
        for entry in normalized:
            entry.pop("scrape_time", None)
    elif isinstance(normalized, dict):
        normalized.pop("scrape_time", None)
    return normalized

def remove_non_unique_json_files(directory, normalize_fn):
    """
    Remove JSON files in `directory` that are duplicates *after normalization*.
    Files are processed in timestamp-sorted order to allow O(N) duplicate removal.
    """
    if not os.path.isdir(directory):
        return

    last_unique = None
    files_processed = 0
    files_deleted = 0

    # Files are already named like qubits_YYYY-MM-DD_HH-MM-SS.json → sorting works
    for fname in tqdm(sorted(os.listdir(directory))):
        if not fname.endswith(".json"):
            continue
        
        path = os.path.join(directory, fname)
        try:
            with open(path) as f:
                data = ujson.load(f)
        except Exception as e:
            print(f"Error reading {path}: {e}")
            continue

        # Normalize (remove timestamps, scrape_time, reorder keys, etc.)
        normalized = normalize_fn(data)

        if last_unique is None:
            # First file becomes the baseline
            last_unique = normalized
            files_processed += 1
            continue

        # Compare normalized structures directly
        if normalized == last_unique:
            # Duplicate → delete file
            try:
                os.remove(path)
                files_deleted += 1
            except Exception as e:
                print(f"Failed to remove {path}: {e}")
            continue
        
        # Update baseline
        last_unique = normalized
        files_processed += 1

    print(f"Processed {files_processed} unique files, deleted {files_deleted} duplicates in '{directory}'")

def load_json_to_dataframe(directory):
    all_entries = []

    if not os.path.isdir(directory):
        return pd.DataFrame()

    for fname in tqdm(sorted(os.listdir(directory))):
        if fname.endswith(".json"):
            path = os.path.join(directory, fname)
            try:
                with open(path) as f:
                    data = ujson.load(f)
                    if isinstance(data, list):
                        all_entries.extend(data)
                    else:
                        all_entries.append(data)
            except Exception as e:
                print(f"Error reading {path}: {e}")

    # Convert to DataFrame
    if not all_entries:
        return pd.DataFrame()
    df = pd.json_normalize(all_entries)  # flatten nested dicts
    return df, all_entries

def df_to_json(df, raw, output_file):
    if df.empty:
        print("No data")
        return

    # Convert list-valued 'qubits' cells to string
    if 'qubits' in df.columns:
        df['qubits'] = df['qubits'].apply(
            lambda x: str(x) if isinstance(x, list) else x
        )

    # Drop scrape-time columns (case-insensitive)
    cols_to_drop = [c for c in df.columns if 'scrape_time' in c.lower()]
    df_unique = df.drop(columns=cols_to_drop).drop_duplicates()

    # Map back to raw entries using original indices
    unique_indices = df_unique.index
    unique_entries = [raw[i] for i in unique_indices]

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w") as f:
        ujson.dump(unique_entries, f, indent=2)

    print(f"Merged {len(unique_entries)} unique entries (removed duplicates)")

def merge_jsons(directory, output_file):
    remove_non_unique_json_files(directory)
    df, raw = load_json_to_dataframe(directory)
    df_to_json(df, raw, output_file)

# ---------- Merge QUBITS ----------
qubits_dir = "data/qubits"
output_file = "data/merged/qubits.json"

merge_jsons(qubits_dir, output_file)


# ---------- Merge GATES ----------
gates_dir = "data/gates"
output_file = "data/merged/gates.json"

merge_jsons(gates_dir, output_file)