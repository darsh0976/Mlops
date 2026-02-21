import json
import os
import subprocess
import tempfile
import unittest


class ErrorCaseTests(unittest.TestCase):
    def run_cmd(self, args):
        return subprocess.run(args, cwd=os.getcwd(), capture_output=True, text=True)

    def _load(self, p):
        with open(p, encoding='utf-8') as f:
            return json.load(f)

    def test_invalid_config_missing_window(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = os.path.join(td, 'config.yaml')
            data = os.path.join(td, 'data.csv')
            out = os.path.join(td, 'metrics.json')
            log = os.path.join(td, 'run.log')
            with open(cfg, 'w', encoding='utf-8') as f:
                f.write('seed: 42\nversion: "v1"\n')
            with open(data, 'w', encoding='utf-8') as f:
                f.write('timestamp,close\n2024-01-01,10\n')
            r = self.run_cmd(['python','run.py','--input',data,'--config',cfg,'--output',out,'--log-file',log])
            self.assertNotEqual(r.returncode, 0)
            payload = self._load(out)
            self.assertEqual(payload['status'], 'error')
            self.assertIn('missing keys', payload['error_message'])

    def test_empty_csv(self):
        with tempfile.TemporaryDirectory() as td:
            cfg = os.path.join(td, 'config.yaml')
            data = os.path.join(td, 'data.csv')
            out = os.path.join(td, 'metrics.json')
            log = os.path.join(td, 'run.log')
            with open(cfg, 'w', encoding='utf-8') as f:
                f.write('seed: 42\nwindow: 5\nversion: "v1"\n')
            open(data, 'w', encoding='utf-8').close()
            r = self.run_cmd(['python','run.py','--input',data,'--config',cfg,'--output',out,'--log-file',log])
            self.assertNotEqual(r.returncode, 0)
            payload = self._load(out)
            self.assertEqual(payload['status'], 'error')
            self.assertIn('Empty input file', payload['error_message'])


if __name__ == '__main__':
    unittest.main()
