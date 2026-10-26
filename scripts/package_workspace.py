#!/usr/bin/env python3
"""Verify fresh workspace packages, avoiding cached siblings of the same version."""
import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--allow-dirty', action='store_true')
    args = parser.parse_args()
    version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']
    # Cargo identifies the workspace staging registry by its path. A fresh path
    # prevents an earlier, unpublished sibling with the same version being reused.
    with tempfile.TemporaryDirectory(prefix='mqtt-kit-package-') as directory:
        target = Path(directory)
        command = ['cargo', 'package', '--locked', '--workspace', '--all-features', '--target-dir', str(target)]
        if args.allow_dirty:
            command.append('--allow-dirty')
        subprocess.run(command, cwd=ROOT, check=True)
        subprocess.run([sys.executable, str(ROOT / 'scripts/check_packages.py'), '--package-dir', str(target / 'package')], cwd=ROOT, check=True)
        output = ROOT / 'target' / 'package'
        output.mkdir(parents=True, exist_ok=True)
        for name in ('mqtt-kit-core', 'mqtt-kit'):
            filename = f'{name}-{version}.crate'
            shutil.copyfile(target / 'package' / filename, output / filename)
            print(f'Verified artifact: {output / filename}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
