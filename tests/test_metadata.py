import unittest
from pathlib import Path
from unittest.mock import patch
from edzbot.archive import Inspection, Member
from edzbot.metadata import read_metadata

MANIFEST = b'''<?xml version="1.0" encoding="utf-8"?>
<manifest version="2.0"><packages>
<package type="part" key="THB.0060002" name="THB.0060002">
 <items><item type="partxml" name="part" locator="THB.0060002.part.xml"/></items>
</package></packages></manifest>'''
PART = b'''<partsmanagement><part P_ARTICLE_MANUFACTURER="THB" P_ARTICLE_PARTNR="THB.0060002"/></partsmanagement>'''


class MetadataTests(unittest.TestCase):
    def test_one_part(self):
        inspection = Inspection([Member('manifest.xml', len(MANIFEST)),
                                 Member('items/partxml/THB.0060002.part.xml', len(PART))],
                                len(MANIFEST)+len(PART), [])
        with patch('edzbot.metadata.read_member', side_effect=[MANIFEST, PART]):
            result = read_metadata(Path('/tmp/THB.0060002.edz'), inspection, '7z')
        self.assertEqual(result.part_count, 1)
        self.assertEqual(result.brand, 'Theben')
        self.assertEqual(result.part, 'THB.0060002')
        self.assertEqual(result.manifest_version, '2.0')

    def test_multi_part(self):
        m = MANIFEST.replace(b'</packages>', b'<package type="part" key="THB.0060003" /></packages>')
        inspection = Inspection([Member('manifest.xml',len(m))],len(m),[])
        with patch('edzbot.metadata.read_member', return_value=m):
            result=read_metadata(Path('/tmp/THB.edz'),inspection,'7z')
        self.assertEqual(result.part_count,2)
        self.assertEqual(result.group,'Multi-part')


if __name__ == '__main__': unittest.main()
