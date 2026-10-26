"""Regression checks for the security gate: exceptions must never widen silently."""
import copy
from datetime import date
import unittest

from security_audit import evaluate, validate_deny_config


class AuditPolicyTests(unittest.TestCase):
    def setUp(self):
        self.today = date(2026, 10, 26)
        self.finding = {'advisory': {'id': 'RUSTSEC-TEST'}, 'package': {
            'name': 'old-webpki', 'version': '0.1.0', 'checksum': 'reviewed-package'}}
        self.report = {'vulnerabilities': {'list': [self.finding]}, 'warnings': {}}
        self.policy = {'reviewed_parent': {'package': 'parent', 'version': '1.0.0', 'checksum': 'parent-hash'},
                       'exceptions': [{'kind': 'vulnerability', 'advisory': 'RUSTSEC-TEST',
                                       'package': 'old-webpki', 'version': '0.1.0',
                                       'checksum': 'reviewed-package', 'expires': '2027-01-24'}]}
        self.lockfile = {'package': [{'name': 'parent', 'version': '1.0.0', 'checksum': 'parent-hash'}]}

    def errors(self):
        return evaluate(self.report, self.policy, self.lockfile, self.today)[1]

    def test_exact_reviewed_finding_is_accepted(self):
        self.assertEqual(self.errors(), [])

    def test_other_version_of_same_advisory_fails(self):
        self.finding['package']['version'] = '0.2.0'
        self.assertTrue(self.errors())

    def test_changed_checksum_fails(self):
        self.finding['package']['checksum'] = 'changed'
        self.assertTrue(self.errors())

    def test_changed_parent_requires_new_review(self):
        self.lockfile['package'][0]['version'] = '1.0.1'
        self.assertTrue(self.errors())

    def test_expired_exception_fails(self):
        self.today = date(2027, 1, 25)
        self.assertTrue(self.errors())

    def test_warning_without_advisory_is_rejected(self):
        self.report['warnings']['yanked'] = [{'advisory': None, 'package': self.finding['package']}]
        self.assertTrue(self.errors())

    def test_unknown_advisory_or_warning_fails(self):
        finding = copy.deepcopy(self.finding)
        finding['advisory']['id'] = 'RUSTSEC-NEW'
        self.report['warnings']['unsound'] = [finding]
        self.assertTrue(self.errors())


class DenyPolicyTests(unittest.TestCase):
    def test_exact_reviewed_ids_with_reasons_are_accepted(self):
        policy = {'exceptions': [{'advisory': 'RUSTSEC-TEST'}]}
        config = {'advisories': {'ignore': [{'id': 'RUSTSEC-TEST', 'reason': 'reviewed'}]}}
        self.assertEqual(validate_deny_config(policy, config), [])

    def test_extra_ignore_or_reasonless_ignore_is_rejected(self):
        policy = {'exceptions': [{'advisory': 'RUSTSEC-TEST'}]}
        for ignores in [['RUSTSEC-TEST'], [{'id': 'RUSTSEC-TEST'}],
                        [{'id': 'RUSTSEC-TEST', 'reason': 'reviewed'}, {'id': 'RUSTSEC-NEW', 'reason': 'unreviewed'}]]:
            self.assertTrue(validate_deny_config(policy, {'advisories': {'ignore': ignores}}))


if __name__ == '__main__':
    unittest.main()
