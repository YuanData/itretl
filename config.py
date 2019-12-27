# -*- coding: utf-8 -*-
import os

HOME_PATH = r'D:\itretl'

PROPERTIES_DIR = os.path.join(os.path.expanduser('~'), 'properties')
os.makedirs(PROPERTIES_DIR) if not os.path.exists(PROPERTIES_DIR) else None

log_dir = os.path.join(HOME_PATH, 'log')
os.makedirs(log_dir) if not os.path.exists(log_dir) else None

DATA_PATH = os.path.join(HOME_PATH, 'data')
os.makedirs(DATA_PATH) if not os.path.exists(DATA_PATH) else None

FEATHER_PATH = os.path.join(DATA_PATH, 'feather')
os.makedirs(FEATHER_PATH) if not os.path.exists(FEATHER_PATH) else None

OUTPUT_PATH = os.path.join(DATA_PATH, 'output')
os.makedirs(OUTPUT_PATH) if not os.path.exists(OUTPUT_PATH) else None

REPORT_PATH = os.path.join(OUTPUT_PATH, 'report')
os.makedirs(REPORT_PATH) if not os.path.exists(REPORT_PATH) else None

HEATMAP_PATH = os.path.join(REPORT_PATH, 'heatmap')
os.makedirs(HEATMAP_PATH) if not os.path.exists(HEATMAP_PATH) else None

HS8_DIFF_RANK_PATH = os.path.join(OUTPUT_PATH, 'hs8_diff_rank')
os.makedirs(HS8_DIFF_RANK_PATH) if not os.path.exists(HS8_DIFF_RANK_PATH) else None

HEATMAP_L_PATH = os.path.join(OUTPUT_PATH, 'heatmap_L')
os.makedirs(HEATMAP_L_PATH) if not os.path.exists(HEATMAP_L_PATH) else None

export_value_with_gr_share_by_cy = os.path.join(OUTPUT_PATH, 'export_value_with_gr_share_by_country')
os.makedirs(export_value_with_gr_share_by_cy) if not os.path.exists(export_value_with_gr_share_by_cy) else None


def load_properties(path_input):
    import codecs
    dic_properties = {}
    with codecs.open(path_input, "r", "utf-8") as f:
        str_all_lines = f.readlines()
        for item in str_all_lines:
            if item[0] == "#":
                pass
            else:
                lst_key_value = item.strip().split("=")
                if len(lst_key_value) > 2:  # if equals signs appear in the string, deal with them by concatenation
                    key, value = lst_key_value[0], '='.join(lst_key_value[1:])
                else:
                    key, value = lst_key_value[0], lst_key_value[1]
                dic_properties[key] = value
        pass
    return dic_properties


PROPERTIES = load_properties(os.path.join(PROPERTIES_DIR, 'db.txt'))
ds_ip = PROPERTIES['ds_ip']
dstore_ip = PROPERTIES['dstore_ip']
