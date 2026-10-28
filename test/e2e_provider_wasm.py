"""End-to-end: WASM embedded in the model payload + model reuse, on a Nexus emulator.

Not a unit test: needs a reachable deployment and credentials, so it is not in
the pytest set CI runs. Drive it with

    AQORA_URL=https://aqora-internal.io python test/e2e_provider_wasm.py nexus:H2-Emulator

The platform must be the qualified lowercase `provider:name` form — a bare
name is rejected, and a capitalised provider reports "was not found".
"""

import os
import sys

from pytket import Circuit
from pytket.circuit import OpType
from pytket.passes import AutoRebase

from aqora.qpu import QPU

URL = os.environ.get("AQORA_URL", "https://aqora.io")
PLATFORM = sys.argv[1] if len(sys.argv) > 1 else "nexus:H2-Emulator"

# Minimal WASM exporting a no-op function named `init`, which Nexus requires of
# any module a job attaches.
WASM = bytes([
    0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00,              # magic + version
    0x01, 0x04, 0x01, 0x60, 0x00, 0x00,                          # type:   () -> ()
    0x03, 0x02, 0x01, 0x00,                                      # func:   one, type 0
    0x07, 0x08, 0x01, 0x04, 0x69, 0x6E, 0x69, 0x74, 0x00, 0x00,  # export: 'init' -> func 0
    0x0A, 0x04, 0x01, 0x02, 0x00, 0x0B,                          # code:   empty body
])

qpu = QPU(url=URL, platform=PLATFORM)
print(f"platform      {PLATFORM}")
print(f"input_formats {qpu.input_formats}")

circ = Circuit(2, 2).H(0).CX(0, 1).measure_all()
# QPU negotiates the serialization format but does not compile for the target:
# H-series accepts only its native gate set, so rebase before submitting.
AutoRebase({OpType.Rz, OpType.PhasedX, OpType.ZZMax}).apply(circ)
print(f"gates         {sorted({str(c.op.type) for c in circ})}")

# 1 — upload a model carrying the WASM module in its payload
model = qpu.upload(circ, wasm=WASM)
print(f"\n[1] uploaded model id={model.id} format={model.serialization_format}")

# 2 — run a job from that model (reuse: no re-upload)
job1 = qpu.run(model, shots=10)
print(f"[2] job1 id={job1.job_id} (from existing model)")

# 3 — reuse again, as a separate session would via qpu.model(id)
reloaded = qpu.model(model.id)
job2 = qpu.run(reloaded, shots=10)
print(f"[3] job2 id={job2.job_id} (model reloaded by id)")

# 4 — a model already holds its WASM, so passing one here must be rejected
try:
    qpu.run(model, shots=10, wasm=WASM)
    print("[4] FAIL: wasm-on-model was accepted")
except Exception as exc:
    print(f"[4] ok: wasm-on-model rejected -> {type(exc).__name__}: {exc}")

# 5 — results
for name, job in (("job1", job1), ("job2", job2)):
    try:
        print(f"\n[5] {name} counts: {job.counts(timeout=300)}")
    except Exception as exc:
        print(f"[5] {name} -> {type(exc).__name__}: {exc}")
