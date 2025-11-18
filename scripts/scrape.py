import json, os
from qiskit_ibm_runtime import QiskitRuntimeService

ts = os.environ["TS"]
token = os.environ["IBM_TOKEN"]

service = QiskitRuntimeService(
    channel="ibm_quantum_platform",
    token=token
)

# Prepare containers
qubit_data = []
gate_data = []

print("[PY] Starting scrape...")

for backend in service.backends(simulator=False):
    print(f"[PY] Processing backend {backend.name}")
    try:
        props = backend.properties()
        
        # --- Qubit data ---
        for qi in range(len(props.qubits)):
            qprops = props.qubit_property(qi)
            qubit_data.append({
                "backend": backend.name,
                "qubit": qi,
                "properties": {k: float(v[0]) for k, v in qprops.items()},
                "timestamps": {k: str(v[1]) for k, v in qprops.items()},
                "scrape_time": ts
            })
        
        # --- Gate data ---
        for gate in props.gates:
            gate_id = gate.gate
            gate_qubits = gate.qubits
            gprops = props.gate_property(gate=gate_id, qubits=gate_qubits)
            gate_data.append({
                "backend": backend.name,
                "gate": gate_id,
                "qubits": gate_qubits,
                "properties": {k: float(v[0]) for k, v in gprops.items()},
                "timestamps": {k: str(v[1]) for k, v in gprops.items()},
                "scrape_time": ts
            })

    except Exception as e:
        print(f"Skipped backend {backend.name}: {e}")

# Save JSON files
os.makedirs("data", exist_ok=True)
os.makedirs("data/qubits", exist_ok=True)
os.makedirs("data/gates", exist_ok=True)

qubit_file = f"data/qubits/qubits_{ts}.json"
gate_file = f"data/gates/gates_{ts}.json"

with open(qubit_file, "w") as f:
    json.dump(qubit_data, f, indent=2)

with open(gate_file, "w") as f:
    json.dump(gate_data, f, indent=2)

print(f"[PY] Saved {len(qubit_data)} qubit entries to {qubit_file}")
print(f"[PY] Saved {len(gate_data)} gate entries to {gate_file}")
