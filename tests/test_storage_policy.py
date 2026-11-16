import unittest
from edzbot.storage_policy import capacity_reason

G = 1024**3


def row(msg_id, name, kind, size, group=None, index=None):
    return dict(id=msg_id, file_name=name, kind=kind, size_bytes=size,
                group_name=group, volume_index=index, state='new')


class StoragePolicyTests(unittest.TestCase):
    def test_small_single(self):
        a = row(1, 'small.edz', 'edz', 22 * G)
        self.assertIsNone(capacity_reason(a, [a], 60 * G))

    def test_large_single(self):
        a = row(1, 'large.7z', 'archive', 61 * G)
        self.assertIn('exceeds', capacity_reason(a, [a], 60 * G))

    def test_volume_sum(self):
        a = row(1, 'A.7z.001', 'volume', 31 * G, 'a.7z', 1)
        b = row(2, 'A.7z.002', 'volume', 30 * G, 'a.7z', 2)
        self.assertIn('exceeds', capacity_reason(a, [a, b], 60 * G))

    def test_volume_within_cap(self):
        a = row(1, 'A.7z.001', 'volume', 24 * G, 'a.7z', 1)
        b = row(2, 'A.7z.002', 'volume', 26 * G, 'a.7z', 2)
        self.assertIsNone(capacity_reason(a, [a, b], 60 * G))

    def test_collision(self):
        a = row(1, 'A.7z.001', 'volume', 10 * G, 'a.7z', 1)
        b = row(2, 'A.7z.001', 'volume', 10 * G, 'a.7z', 1)
        self.assertIn('Duplicate', capacity_reason(a, [a, b], 60 * G))

    def test_invalid_size(self):
        a = row(1, 'A.edz', 'edz', 0)
        self.assertIn('size', capacity_reason(a, [a], 60 * G))


if __name__ == '__main__':
    unittest.main()
