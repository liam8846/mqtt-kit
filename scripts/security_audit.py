#!/usr/bin/env python3
"""Run RustSec audit with exact package/version/checksum exceptions and expiry."""
import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def evaluate(report, policy, lockfile, today):
    errors = []
    accepted = []
    parent = policy['reviewed_parent']
    parent_reviewed = any(
        p['name'] == parent['package'] and p['version'] == parent['version']
        and p.get('checksum') == parent['checksum'] for p in lockfile['package']
    )
    findings = [('vulnerability', v) for v in report['vulnerabilities']['list']]
    for kind, values in report.get('warnings', {}).items():
        findings.extend((kind, value) for value in values)
    # Policy expiry is intentional even if a dependency disappears: remove obsolete exceptions.
    for exception in policy['exceptions']:
        if today > date.fromisoformat(exception['expires']):
            errors.append(f"Expired exception: {exception['advisory']} ({exception['expires']})")
    for kind, finding in findings:
        advisory = (finding.get('advisory') or {}).get('id', f'NO-ADVISORY:{kind}')
        package = finding['package']
        label = f"{advisory}: {package['name']} {package['version']} ({kind})"
        matches = [e for e in policy['exceptions'] if (
            e['kind'] == kind and e['advisory'] == advisory
            and e['package'] == package['name'] and e['version'] == package['version']
            and e['checksum'] == package.get('checksum')
            and today <= date.fromisoformat(e['expires'])
        )]
        if matches and parent_reviewed:
            accepted.append(f"{label}; expires {matches[0]['expires']}")
        else:
            errors.append(label)
    return accepted, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', help='Optional RustSec database path')
    parser.add_argument('--no-fetch', action='store_true', help='Use an already fetched database')
    parser.add_argument('--output', type=Path, help='Save the raw audit JSON')
    args = parser.parse_args()
    command = ['cargo', 'audit', '--json', '--file', str(ROOT / 'Cargo.lock')]
    if args.db:
        command.extend(['--db', args.db])
    if args.no_fetch:
        command.append('--no-fetch')
    result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False)
    if result.returncode not in (0, 1):
        print(result.stderr or result.stdout, file=sys.stderr)
        return 1
    try:
        report = json.loads(result.stdout)
        if report.get('error'):
            raise ValueError(str(report['error']))
        policy = json.loads((ROOT / 'scripts/security-exceptions.json').read_text())
        lockfile = tomllib.loads((ROOT / 'Cargo.lock').read_text())
        accepted, errors = evaluate(report, policy, lockfile, datetime.now(timezone.utc).date())
    except (ValueError, KeyError, TypeError) as error:
        print(f'Audit failed: {error}\n{result.stderr}', file=sys.stderr)
        return 1
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    for item in accepted:
        print(f'REVIEWED: {item}')
    for item in errors:
        print(f'FAIL: {item}', file=sys.stderr)
    print(f'{len(accepted)} reviewed findings; {len(errors)} unaccepted findings.')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
