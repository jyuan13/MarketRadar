# -*- coding:utf-8 -*-
import json
import numpy as np
import os

class NpEncoder(json.JSONEncoder):
    """
    Numpy JSON Encoder
    """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

class JsonFormatter:
    """
    JSON Formatting Layer
    """
    
    @staticmethod
    def save_compact_json(data, filename, ensure_ascii=False):
        """
        Save JSON with compact formatting for lists (pseudo-compact).
        Actually standard json.dump is fine, but if we want specific compact style
        (like all numbers in one line), we might need custom logic.
        For now, standard pretty print is sufficient or just standard compact.
        Legacy code used ensure_ascii=False.
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, cls=NpEncoder, ensure_ascii=ensure_ascii, indent=4)
            return True, f"Saved to {filename}"
        except Exception as e:
            return False, str(e)
