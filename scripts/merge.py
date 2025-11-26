import os, ujson

# ---------- Helper ----------
def load_all_json(directory):
    result = []
    if not os.path.isdir(directory):
        return result
    for fname in sorted(os.listdir(directory)):
        if fname.endswith(".json"):
            path = os.path.join(directory, fname)
            try:
                with open(path) as f:
                    data = ujson.load(f)
                    if isinstance(data, list):
                        result.extend(data)
                    else:
                        result.append(data)
            except Exception as e:
                print(f"Error reading {path}: {e}")
    return result

# ---------- Merge QUBITS ----------
qubits = load_all_json("data/qubits")
with open("merged/qubits_hourly.json", "w") as f:
    ujson.dump(qubits, f, indent=2)

# ---------- Merge GATES ----------
gates = load_all_json("data/gates")
with open("merged/gates_hourly.json", "w") as f:
    ujson.dump(gates, f, indent=2)

print(f"Merged {len(qubits)} qubit entries")
print(f"Merged {len(gates)} gate entries")