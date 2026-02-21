import json
import os
import subprocess
import tempfile
import unittest


class RunPyIntegrationTests(unittest.TestCase):
    def run_cmd(self, args, cwd):
        return subprocess.run(args, cwd=cwd, capture_output=True, text=True)

    def load_json(self, path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def test_success_path(self):
        with tempfile.TemporaryDirectory() as td:
            config = os.path.join(td, "config.yaml")
            data = os.path.join(td, "data.csv")
            out = os.path.join(td, "metrics.json")
            log = os.path.join(td, "run.log")

            with open(config, "w", encoding="utf-8") as f:
                f.write('seed: 42\nwindow: 3\nversion: "v1"\n')

            with open(data, "w", encoding="utf-8") as f:
                f.write("timestamp,open,high,low,close,volume_btc,volume_usd\n")
                f.write("2024-01-01 00:00:00,1,1,1,1,1,1\n")
                f.write("2024-01-01 00:01:00,2,2,2,2,1,2\n")
                f.write("2024-01-01 00:02:00,3,3,3,3,1,3\n")
                f.write("2024-01-01 00:03:00,4,4,4,4,1,4\n")

            res = self.run_cmd([
                "python",
                "run.py",
                "--input",
                data,
                "--config",
                config,
                "--output",
                out,
                "--log-file",
                log,
            ], cwd=os.getcwd())

            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertTrue(os.path.exists(out))
            self.assertTrue(os.path.exists(log))

            payload = self.load_json(out)
            self.assertEqual(payload["status"], "success")
            self.assertEqual(payload["rows_processed"], 4)
            self.assertEqual(payload["metric"], "signal_rate")
            self.assertIn("latency_ms", payload)

    def test_missing_input_file_error(self):
        with tempfile.TemporaryDirectory() as td:
            config = os.path.join(td, "config.yaml")
            out = os.path.join(td, "metrics.json")
            log = os.path.join(td, "run.log")

            with open(config, "w", encoding="utf-8") as f:
                f.write('seed: 42\nwindow: 5\nversion: "v1"\n')

            res = self.run_cmd([
                "python",
                "run.py",
                "--input",
                os.path.join(td, "missing.csv"),
                "--config",
                config,
                "--output",
                out,
                "--log-file",
                log,
            ], cwd=os.getcwd())

            self.assertNotEqual(res.returncode, 0)
            payload = self.load_json(out)
            self.assertEqual(payload["status"], "error")
            self.assertIn("Missing input file", payload["error_message"])

    def test_missing_close_column_error(self):
        with tempfile.TemporaryDirectory() as td:
            config = os.path.join(td, "config.yaml")
            data = os.path.join(td, "data.csv")
            out = os.path.join(td, "metrics.json")
            log = os.path.join(td, "run.log")

            with open(config, "w", encoding="utf-8") as f:
                f.write('seed: 42\nwindow: 5\nversion: "v1"\n')

            with open(data, "w", encoding="utf-8") as f:
                f.write("timestamp,open,high,low,volume_btc,volume_usd\n")
                f.write("2024-01-01 00:00:00,1,1,1,1,1\n")

            res = self.run_cmd([
                "python",
                "run.py",
                "--input",
                data,
                "--config",
                config,
                "--output",
                out,
                "--log-file",
                log,
            ], cwd=os.getcwd())

            self.assertNotEqual(res.returncode, 0)
            payload = self.load_json(out)
            self.assertEqual(payload["status"], "error")
            self.assertIn("Missing required columns", payload["error_message"])

    def test_deterministic_output_value(self):
        with tempfile.TemporaryDirectory() as td:
            config = os.path.join(td, "config.yaml")
            data = os.path.join(td, "data.csv")
            out1 = os.path.join(td, "metrics1.json")
            out2 = os.path.join(td, "metrics2.json")
            log1 = os.path.join(td, "run1.log")
            log2 = os.path.join(td, "run2.log")

            with open(config, "w", encoding="utf-8") as f:
                f.write('seed: 42\nwindow: 3\nversion: "v1"\n')

            with open(data, "w", encoding="utf-8") as f:
                f.write("timestamp,open,high,low,close,volume_btc,volume_usd\n")
                for i, close in enumerate([10, 11, 12, 11, 10, 9, 10, 11]):
                    f.write(f"2024-01-01 00:0{i}:00,1,1,1,{close},1,1\n")

            res1 = self.run_cmd([
                "python", "run.py", "--input", data, "--config", config, "--output", out1, "--log-file", log1
            ], cwd=os.getcwd())
            res2 = self.run_cmd([
                "python", "run.py", "--input", data, "--config", config, "--output", out2, "--log-file", log2
            ], cwd=os.getcwd())

            self.assertEqual(res1.returncode, 0)
            self.assertEqual(res2.returncode, 0)
            payload1 = self.load_json(out1)
            payload2 = self.load_json(out2)
            self.assertEqual(payload1["value"], payload2["value"])


if __name__ == "__main__":
    unittest.main()
