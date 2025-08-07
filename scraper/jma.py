import json
import os
import requests


def load_area_codes(path='areacode.txt'):
    """
    Load mapping of region names to JMA area codes from a CSV-style file.
    Each line: <Prefecture>: <Region Name>,<AreaCode>
    """
    codes = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(',')
            if len(parts) < 2:
                continue
            name = ','.join(parts[:-1]).strip()
            code = parts[-1].strip()
            codes[name] = code
    return codes


def load_content_mapping(path='content.txt'):
    """
    Load JMA phenomenon mappings (value -> (ja, en)) from a JSON file.
    Expects an array of sections; finds the one where key == 'elem'.
    """
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    for section in data:
        if section.get('key') == 'elem':
            return {
                v['value']: (v.get('name'), v.get('enName'))
                for v in section.get('values', [])
            }
    return {}


def fetch_warnings(area_code):
    """
    Retrieve the raw GeoJSON warning data for a given area code.
    """
    url = f'https://www.jma.go.jp/bosai/warning/data/warning/{area_code}.json'
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json().get('features', [])


def parse_warnings(features, mapping):
    """
    From a list of GeoJSON features, extract only those with level == 'Warning',
    mapping the phenomenon code to Japanese and English names.
    Returns a list of dicts: time, area, phenomenon_ja, phenomenon_en, level.
    """
    warnings = []
    for feat in features:
        props = feat.get('properties', {})
        level = props.get('level')
        if level != 'Warning':  # filter out advisories, watches, etc.
            continue
        ph = props.get('phenomenon')
        ja, en = mapping.get(ph, (ph, ph))
        area = props.get('areaName') or props.get('area', '')
        time = props.get('time')
        warnings.append({
            'time': time,
            'area': area,
            'phenomenon_ja': ja,
            'phenomenon_en': en,
            'level': level,
        })
    return warnings


def get_all_warnings(area_codes_path='areacode.txt', content_path='content.txt'):
    """
    Load all area codes and phenomenon mappings, fetch and parse warnings for each,
    and return a dict mapping region names to lists of warning entries.
    """
    codes = load_area_codes(area_codes_path)
    mapping = load_content_mapping(content_path)
    output = {}
    for region, code in codes.items():
        feats = fetch_warnings(code)
        ws = parse_warnings(feats, mapping)
        if ws:
            output[region] = ws
    return output


if __name__ == '__main__':
    # Quick CLI: prints all current warnings
    warnings = get_all_warnings()
    for region, items in warnings.items():
        for w in items:
            print(f"{w['time']} — {region} — {w['phenomenon_en']} [{w['level']}]  {w['phenomenon_ja']}")
