# import build in modules

import json
import pandas as pd
from pandas import json_normalize
import os
import sys
import numpy as np
import hashlib
sys.path.append("./")
# import custom module
from config.hubspot.hubspotoop import HubspotAPI
from datetime import datetime, timezone


def extract_properties(objects):
    hs=HubspotAPI()
    data=hs.object_properties(objects=objects)
    names_array = [obj["name"] for obj in data]
    return (names_array)
