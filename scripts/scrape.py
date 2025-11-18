import json, os
from qiskit_ibm_runtime import QiskitRuntimeService

ts = os.environ["TS"]
token = os.environ["IBM_TOKEN"]

service = QiskitRuntimeService(
    channel="ibm_quantum_platform",
    token=token
)

data = []

for backend in service.backends(simulator=False):
    try:
        props = backend.properties()
        for qi in range(len(props.qubits)):
            qprops = props.qubit_property(qi)
            entry = {
                "backend": backend.name,
                "qubit": qi
            }
            for key, (value, ts_v) in qprops.items():
                entry[key] = {
                    "value": float(value),
                    "timestamp": ts_v.isoformat()
                }
            data.append(entry)
    except Exception:
        pass

os.makedirs("data", exist_ok=True)
with open(f"data/calibration_{ts}.json", "w") as f:
    json.dump(data, f, indent=2)
