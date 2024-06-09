from datetime import datetime,timezone
import json
import uuid

def traverse_dict(dictionary):
    data = []
    for key, value in dictionary.items():
        if isinstance(value, dict):
            inner_dict = {'parent_key': key}
            for inner_key, inner_value in value.items():
                inner_dict[inner_key] = inner_value
            data.append(inner_dict)
        else:
            data.append({key: value})
    return data


def genegrate_emitted_info(data_dict) :
    try:
        current_utc_date = datetime.now(timezone.utc)
        emitted_at = current_utc_date.strftime("%Y-%m-%d %H:%M:%S")
        emitted_id = str(uuid.uuid4())
        for d in data_dict:
            d['emitted_at']=emitted_at
            d['emitted_id']=emitted_id
        return data_dict
    except Exception as e:
        print("Error in genegrate_emitted_info: ", str(e))
        return None