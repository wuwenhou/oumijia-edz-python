import unittest
from edzbot.archive import file_kind, safe_member, safe_basename
from edzbot.metadata import brand_from_filename, folder_segment, tag


class ArchiveTests(unittest.TestCase):
    def test_extensions(self):
        self.assertEqual(file_kind('DELTA.7z.003'), ('volume', 'DELTA.7z', 3))
        self.assertEqual(file_kind('EKF.edz'), ('edz', None, None))
        self.assertEqual(file_kind('Theben.7z'), ('archive', None, None))
        self.assertEqual(file_kind('file.pdf')[0], None)

    def test_security(self):
        for name in ('../secret', '/etc/passwd', 'C:\\Windows\\win.ini', 'a/../../secret', '.','a//b'):
            self.assertFalse(safe_member(name), name)
        self.assertTrue(safe_member('Theben/THB.0060002.edz'))
        self.assertEqual(safe_basename('Theben/THB.0060002.edz'), 'THB.0060002.edz')

    def test_classification(self):
        self.assertEqual(brand_from_filename('THB.0060002.edz'), 'Theben')
        self.assertEqual(brand_from_filename('EKF.10201.edz'), 'EKF')
        self.assertEqual(folder_segment('a/../../b'), 'a_.._.._b')
        self.assertEqual(tag('Schneider Electric'), 'Schneider_Electric')


if __name__ == '__main__': unittest.main()
