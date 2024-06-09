import pandas as pd 
import sys
import re
import json
sys.path.append("./")
from pipeline.functions.functions import concat_df_column_into_string
from config.redash.Redash import Redash
from config.hubspot.hubspotoop import HubspotAPI

def extract_group_id(text):
    pattern = r'group_id: (\w+-\w+-\w+-\w+-\w+)'
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    else:
        return None
    
def concat_group_ids(note_engagements):
    # Wrap each group_id value with parentheses and join them into a single string separated by commas
    wrapped_group_ids = ','.join([f"('{group_id}')" for group_id in note_engagements['group_id']])

    return wrapped_group_ids

def get_group_creator(note_engagements):
    batch_size = 1000
    num_batches = (len(note_engagements) + batch_size - 1) // batch_size
    group_creator_dfs = []
   
   
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(note_engagements))
        batch_df = note_engagements.iloc[start_idx:end_idx]

        group_ids_str = group_ids_str=concat_group_ids(batch_df)

        # Assuming concat_df_column_into_string concatenates elements into string
        rd = Redash()
        group_creator_batch_df = rd.get_fresh_query_result(query_id="774", params={"max_age": 0, "parameters": {"group_id": group_ids_str}})
        group_creator_batch_df = pd.DataFrame(group_creator_batch_df)
        group_creator_batch_df = group_creator_batch_df[['group_id', 'email']]
        group_creator_dfs.append(group_creator_batch_df)

    # Concatenating all batch results into one DataFrame
    final_group_creator_df = pd.concat(group_creator_dfs, ignore_index=True)
    merge_df=note_engagements.merge(final_group_creator_df, on='group_id', how='left')
    return merge_df



note_engagements=pd.read_csv("data\processed\company_note_engagement.csv")
note_engagements['group_id'] = note_engagements['company_note'].apply(extract_group_id)
filtered_note_engagements = note_engagements[note_engagements['group_id'].notnull()]
merge_df=get_group_creator(filtered_note_engagements)
merge_df['company_note']=merge_df['company_note']+", created by: " + merge_df['email']
pd.set_option('display.max_colwidth', None)
print(merge_df['company_note'].head(20))
hb=HubspotAPI()
for index,row in merge_df.iterrows():
    note_id=row['id']
    note=row['company_note']
    update_param=json.dumps({"properties":{"hs_note_body":f"{note}"}})
    object_type="notes"
    hb.patch_update_object(object_type,note_id,update_param)