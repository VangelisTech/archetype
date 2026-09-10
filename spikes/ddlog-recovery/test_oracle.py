"""Transport-free contracts for the independent acceptance oracle."""
import unittest
from connected_components_oracle import apply, expected, rows


class OracleTests(unittest.TestCase):
    def test_reverse_edge_retains_support_until_both_removed(self):
        facts = {'vertices': {(1,), (2,)}, 'edges': {(1, 2), (2, 1)}}
        first = apply(facts, [{'op': 'delete', 'predicate': 'edges', 'values': [1, 2]}])
        self.assertEqual(expected(first), {(1, 1), (2, 1)})
        second = apply(first, [{'op': 'delete', 'predicate': 'edges', 'values': [2, 1]}])
        self.assertEqual(expected(second), {(1, 1), (2, 2)})
        self.assertEqual(facts['edges'], {(1, 2), (2, 1)})

    def test_invalid_batch_does_not_mutate_input(self):
        facts = {'vertices': set(), 'edges': set()}
        with self.assertRaises(ValueError):
            apply(facts, [{'op': 'insert', 'predicate': 'vertices', 'values': [1]},
                          {'op': 'insert', 'predicate': 'edges', 'values': [True, 2]}])
        self.assertEqual(facts, {'vertices': set(), 'edges': set()})

    def test_rows_reject_malformed_and_duplicate_output(self):
        row = 'R_labels{.f0 = -1, .f1 = -2}'
        self.assertEqual(rows(row), {(-1, -2)})
        for text in (row + '\n' + row, row + ' trailing', 'garbage'):
            with self.assertRaises(ValueError):
                rows(text)


if __name__ == '__main__':
    unittest.main()
