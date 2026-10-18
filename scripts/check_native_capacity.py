"""Bound native subprocesses and retain only safe synthetic loopback outcomes."""

import json
import os
from pathlib import Path
import subprocess
import sys


def main(output):
    trials = []
    for calls, waiting in ((5, 2), (8, 0)):
        try:
            process = subprocess.run(
                [sys.executable, "scripts/sip_capacity.py", str(calls), str(waiting)],
                capture_output=True,
                text=True,
                timeout=45,
            )
            results = [
                json.loads(line.removeprefix("SIP_CAPACITY_RESULT="))
                for line in process.stdout.splitlines()
                if line.startswith("SIP_CAPACITY_RESULT=")
            ]
            result = results[0] if len(results) == 1 else {"passed": False}
            result.update(process_exit=process.returncode, native_abort=process.returncode < 0)
        except subprocess.TimeoutExpired:
            result = {"passed": False, "error_type": "SUBPROCESS_TIMEOUT"}
        result.update(calls=calls, waiting=waiting)
        trials.append(result)
    report = {"kind": "SYNTHETIC_REAL_NATIVE_SIP_RTP_NO_PROVIDERS", "trials": trials}
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write("Native SIP/RTP acceptance\n\n```json\n")
            stream.write(json.dumps(report, indent=2))
            stream.write("\n```\n")
    print(json.dumps(report))
    return 0 if all(row.get("passed") and row.get("process_exit") == 0 for row in trials) else 1


if __name__ == "__main__":
    raise SystemExit(main(Path(sys.argv[1])))
