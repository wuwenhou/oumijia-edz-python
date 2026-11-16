"""Metadata extraction from EPLAN manifest and one part-XML, no EPLAN runtime."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import re
from defusedxml import ElementTree as ET
from .archive import Member, Inspection, read_member

BRANDS = {
    'THB': 'Theben', 'EKF': 'EKF', 'SIE': 'Siemens', 'SIEMENS': 'Siemens',
    'SCH': 'Schneider Electric', 'SCHNEIDER': 'Schneider Electric',
    'RIT': 'Rittal', 'FES': 'Festo', 'ABB': 'ABB', 'DELTA': 'Delta',
    'LS': 'LS Electric', 'OMR': 'Omron', 'MITSUBISHI': 'Mitsubishi Electric',
}


@dataclass(frozen=True)
class Metadata:
    part_count: int
    part_numbers: tuple[str, ...]
    brand: str
    part: str
    group: str
    manifest_version: str


def localname(tag: str) -> str:
    return tag.rsplit('}', 1)[-1].lower()


def brand_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    code = re.split(r'[.\- _]+', stem)[0].upper()
    return BRANDS.get(code, 'Unknown')


def manufacturer_from_attrs(root) -> str:
    for node in root.iter():
        if localname(node.tag) not in ('part', 'article', 'manufacturer'):
            continue
        fields = {k.upper(): v for k,v in node.attrib.items()}
        for key in ('P_ARTICLE_MANUFACTURER_NAME', 'P_ARTICLE_MANUFACTURER', 'MANUFACTURER'):
            value = fields.get(key, '').strip()
            if value:
                return BRANDS.get(value.upper(), value[:100])
    return ''


def read_metadata(archive: Path, inspection: Inspection, sevenzip: str) -> Metadata:
    manifest_entries = [m for m in inspection.members if m.path.replace('\\','/').lower() == 'manifest.xml']
    if not manifest_entries:
        raise ValueError('EPLAN manifest.xml missing')
    root = ET.fromstring(read_member(archive, sevenzip, manifest_entries[0], max_bytes=64*1024*1024))
    if localname(root.tag) != 'manifest':
        raise ValueError('Unexpected manifest root')
    packages = [node for node in root.iter() if localname(node.tag) == 'package']
    parts = [node for node in packages if node.attrib.get('type','').lower() == 'part']
    keys = tuple(x.attrib.get('key','').strip() for x in parts)
    if not keys:
        raise ValueError('EDZ contains no part packages')
    brand = brand_from_filename(archive.name)
    part = keys[0] if len(keys) == 1 else ''
    group = 'Multi-part' if len(keys)>1 else 'Uncategorized'
    if len(keys) == 1:
        for item in parts[0].iter():
            if localname(item.tag) != 'item' or item.attrib.get('name','').lower() != 'part':
                continue
            loc = item.attrib.get('locator','').replace('\\','/')
            key = 'items/partxml/' + loc
            match = next((m for m in inspection.members if m.path.replace('\\','/').casefold() == key.casefold()), None)
            if match:
                try:
                    part_root = ET.fromstring(read_member(archive, sevenzip, match, max_bytes=16*1024*1024))
                    b = manufacturer_from_attrs(part_root)
                    if b: brand = b
                    for node in part_root.iter():
                        if localname(node.tag) == 'part':
                            attrs = {k.upper():v for k,v in node.attrib.items()}
                            group = attrs.get('P_ARTICLE_PRODUCTGROUP') or attrs.get('P_ARTICLE_PRODUCTTOPGROUP') or group
                            break
                except (ValueError, ET.ParseError):
                    pass
            break
    if brand == 'Unknown':
        brand = brand_from_filename(keys[0])
    return Metadata(len(keys), keys, brand, part, group, root.attrib.get('version',''))


def folder_segment(value: str, default: str = 'Unknown') -> str:
    value = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', value.strip())
    value = value.strip('. ')[:85]
    return value or default


def tag(value: str) -> str:
    text = re.sub(r'[^\w]+', '_', value, flags=re.UNICODE).strip('_')[:50]
    return text or 'Unknown'
