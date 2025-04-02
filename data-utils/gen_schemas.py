import os
import json
import subprocess
import pathlib

for dir in os.listdir("tests/data/files"):
    pathlib.Path(f"tests/data/schema/{dir}").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"tests/data/config/{dir}").mkdir(parents=True, exist_ok=True)
    for file in os.listdir(f"tests/data/files/{dir}"):
        filename = file.split(".")[0]
        input = f"tests/data/files/{dir}/{file}"
        output = f"tests/data/schema/{dir}/{filename}.json"
        try:
            result = subprocess.check_output(
                ["aqora", "data", "infer", "--output", "json", input]
            ).decode("utf8")

        except subprocess.CalledProcessError:
            result = '{ "error": true }'
        with open(output, "w") as f:
            f.write(result)
