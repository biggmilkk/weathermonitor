def _load_areacode_map() -> Dict[str, str]:
    """
    Build area_code -> 'Prefecture: Region' from scraper/areacode.json.
    Supports:
      1) flat dict: { "014010": "Hokkaido: Kushiro Region", ... }
      2) dict of dicts with fields like { "pref_en", "pref", "name_en", "name", "label" }
      3) nested under "areas"/"regions"/"offices"
    """
    import json, logging
    from pathlib import Path

    cand = [
        Path(__file__).resolve().parent / "areacode.json",  # scraper/areacode.json
        Path("/mnt/data/areacode.json"),
        Path("areacode.json"),
    ]

    def norm_name(d: dict) -> Optional[str]:
        # Try English first, then Japanese
        pref = d.get("pref_en") or d.get("pref") or d.get("prefecture_en") or d.get("prefecture")
        region = d.get("name_en") or d.get("name") or d.get("label")
        if pref and region:
            return f"{pref}: {region}"
        # Fallback to any single field
        return d.get("name_en") or d.get("name") or d.get("label")

    for p in cand:
        try:
            if not p.exists():
                continue
            data = json.loads(p.read_text(encoding="utf-8"))

            # Case 1: flat dict of strings
            if isinstance(data, dict) and all(isinstance(k, (str, int)) for k in data.keys()) and all(
                isinstance(v, str) for v in data.values()
            ):
                return {str(k): v for k, v in data.items()}

            # Case 2: dict of dicts
            if isinstance(data, dict):
                m = {}
                for k, v in data.items():
                    if isinstance(v, dict):
                        nm = norm_name(v)
                        if nm:
                            m[str(k)] = nm
                if m:
                    return m

                # Case 3: nested common containers
                for top in ("areas", "regions", "offices"):
                    if top in data and isinstance(data[top], dict):
                        m = {}
                        for k, v in data[top].items():
                            if isinstance(v, str):
                                m[str(k)] = v
                            elif isinstance(v, dict):
                                nm = norm_name(v)
                                if nm:
                                    m[str(k)] = nm
                        if m:
                            return m

        except Exception as e:
            logging.warning("[JMA DEBUG] Failed reading areacode.json at %s: %s", p, e)

    logging.warning("[JMA DEBUG] areacode.json not found/parsed; region names may show as raw codes.")
    return {}
