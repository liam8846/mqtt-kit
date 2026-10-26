#!/usr/bin/env python3
"""Validate the contents of Cargo-produced packages without publishing them."""
from pathlib import Path
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
workspace = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']
version = workspace['version']

for name in ('mqtt-kit-core', 'mqtt-kit'):
    prefix = f'{name}-{version}'
    archive = ROOT / 'target' / 'package' / f'{prefix}.crate'
    with tarfile.open(archive, 'r:gz') as package:
        def read(relative):
            stream = package.extractfile(f'{prefix}/{relative}')
            if stream is None:
                raise ValueError(f'Missing package file: {relative}')
            return stream.read()

        manifest = tomllib.loads(read('Cargo.toml').decode())
        assert manifest['package']['version'] == version
        assert manifest['package']['license'] == 'MIT OR Apache-2.0'
        assert manifest['package']['repository'] == workspace['repository']
        assert manifest['package']['readme'] == 'README.md'
        for license_name in ('LICENSE-MIT', 'LICENSE-APACHE'):
            assert read(license_name) == (ROOT / license_name).read_bytes(), license_name
        readme = read('README.md').decode()
        assert '../../' not in readme, f'{name}: broken links to workspace root'
        assert workspace['repository'] in readme
        if name == 'mqtt-kit':
            dependency = manifest['dependencies']['mqtt-kit-core']
            assert dependency['version'] == version
            assert 'path' not in dependency
        for member in package.getnames():
            assert '/.env' not in member and '/.aws/' not in member, member
            assert not member.endswith(('.key', '.pem', '.p12', '.pfx')), member
        print(f'PASS: {archive.name}: licenses, metadata, README, dependency versions and contents')
