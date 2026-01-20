import os
import ujson
import json
from copy import deepcopy
import gzip
from tqdm import tqdm
import hashlib
import shutil

def qubit_key(e):
    return f"{e['backend']}|{e['qubit']}"

def gate_key(e):
    q = ",".join(map(str, e["qubits"]))
    return f"{e['backend']}|{e['gate']}|{q}"

def load_state(path):
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return ujson.load(f)

def save_state(path, state):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        ujson.dump(state, f)

def load_last(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return f.read().strip()

def save_last(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(value)

def normalized_hash(entry):
    e = dict(entry)
    e.pop("scrape_time", None)
    e.pop("timestamps", None)
    key = json.dumps(e, sort_keys=True).encode()
    return hashlib.sha256(key).hexdigest()


def incremental_merge(directory, merged_file, state_file, last_path, key_func):

    files = sorted(f for f in os.listdir(directory) if f.endswith(".json"))

    last_file = load_last(last_path)

    if last_file is None:
        new_files = files
    else:
        new_files = [f for f in files if f > last_file]
    
    if not new_files:
        print("No new files to process.")
        return


    print("Number of files to process:", len(new_files))

    state = load_state(state_file)

    nb_static = 0

    print("Merging into:", merged_file," . . .")

    with gzip.open(merged_file, "at") as out:

        for fname in tqdm(new_files) :
            path = os.path.join(directory, fname)
            with open(path) as f:
                data = ujson.load(f)
            
            entries = data if isinstance(data, list) else [data]

            changed = False
            for entry in entries:
                k = key_func(entry)
                h = normalized_hash(entry)

                if state.get(k) != h:
                    state[k] = h
                    changed = True
                    out.write(ujson.dumps(entry))
                    out.write("\n")

                else :
                    nb_static += 1
                    continue   # unchanged entity

            if not changed:
                os.remove(path)
                
            last_file = fname

    save_state(state_file, state)    
    save_last(last_path, last_file)

def build_all_file(output_path, *inputs):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with gzip.open(output_path, "wb") as out:
        for path in inputs:
            if not os.path.exists(path):
                continue

            with gzip.open(path, "rb") as src:
                shutil.copyfileobj(src, out)

    print("Built:", output_path)


gates_directory = "data/gates"

gates_merged_path = "data/merged/gates.json.gz"

gates_state_file = "data/merged/state_gates.json"

gates_last_path = "data/merged/last_gates.txt"


qubits_directory = "data/qubits"

qubits_merged_path = "data/merged/qubits.json.gz"

qubits_state_file = "data/merged/state_qubits.json"

qubits_last_path = "data/merged/last_qubits.txt"

incremental_merge(gates_directory, gates_merged_path, gates_state_file, gates_last_path, gate_key)
incremental_merge(qubits_directory, qubits_merged_path, qubits_state_file, qubits_last_path, qubit_key)

build_all_file("data/merged/all_data.json.gz", gates_merged_path, qubits_merged_path)
