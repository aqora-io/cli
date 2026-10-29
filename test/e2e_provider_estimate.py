"""End-to-end: HQC cost estimation through a Nexus syntax checker.

Not a unit test: needs a reachable deployment and credentials, so it is not in
the pytest set CI runs. Drive it with

    AQORA_URL=http://localhost:8080 python test/e2e_provider_estimate.py nexus:H2-1E

Estimating is free: the programs go to the platform's syntax checker (H2-1SC
for H2-1 and H2-1E), which compiles them without running them. Pass `--run` to
also run the estimated model on the platform itself and compare the cost Nexus
reports; that one consumes HQCs.

The platform must be the qualified lowercase `provider:name` form — a bare
name is rejected, and a capitalised provider reports "was not found".
"""

import os
import sys

from pytket import Circuit
from pytket.circuit import OpType
from pytket.passes import AutoRebase

from aqora.qpu import QPU

# Required rather than defaulted: this submits jobs, so it must never reach a
# deployment by accident.
URL = os.environ.get("AQORA_URL")
if not URL:
    sys.exit("AQORA_URL must be set, e.g. AQORA_URL=http://localhost:8080")
ARGS = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
PLATFORM = ARGS[0] if ARGS else "nexus:H2-1E"
RUN = "--run" in sys.argv
SHOTS = 100

qpu = QPU(url=URL, platform=PLATFORM)
print(f"platform      {PLATFORM}")
print(f"input_formats {qpu.input_formats}")

circ = Circuit(2, 2).H(0).CX(0, 1).measure_all()
# QPU negotiates the serialization format but does not compile for the target:
# H-series accepts only its native gate set, so rebase before submitting.
AutoRebase({OpType.Rz, OpType.PhasedX, OpType.ZZMax}).apply(circ)
print(f"gates         {sorted({str(c.op.type) for c in circ})}")

# 1 — estimate programs directly: uploaded for this estimate alone
cost = qpu.estimate(circ, shots=SHOTS, timeout=300)
print(f"\n[1] estimate of the circuit, {SHOTS} shots: {cost} HQC")

# 2 — upload once, estimate the model, at two shot counts
model = qpu.upload(circ)
print(f"\n[2] uploaded model id={model.id} format={model.serialization_format}")
for shots in (SHOTS, 10 * SHOTS):
    print(f"    estimate, {shots} shots: {qpu.estimate(model, shots=shots, timeout=300)} HQC")

# 3 — a platform without a syntax checker must be refused
try:
    QPU(url=URL, platform="nexus:Selene").estimate(circ, shots=SHOTS)
    print("\n[3] FAIL: estimate on Selene was accepted")
except Exception as exc:
    print(f"\n[3] ok: Selene refused -> {type(exc).__name__}: {exc}")

# 4 — optionally run the same model for real and compare with the estimate
if RUN:
    job = qpu.run(model, shots=SHOTS)
    print(f"\n[4] job id={job.job_id} on {PLATFORM}")
    print(f"    counts: {job.counts(timeout=1800)}")
    print(f"    actual cost: {job.hqc_cost()} HQC (estimated {cost})")
else:
    print("\n[4] skipped: pass --run to run the model and compare (consumes HQCs)")
