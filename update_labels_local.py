from pymongo import MongoClient, errors
import pandas as pd
from dotenv import load_dotenv
import logging
import os
from utils import get_s3_client, download_file

logging.basicConfig(
   level=logging.INFO,  # Set the logging level
   format="%(asctime)s - %(levelname)s - %(message)s",
   handlers=[
   logging.FileHandler("logs/update_labels.log", encoding='utf-8'),
   ]
)

load_dotenv()
MONGODB_URI = os.environ['MONGODB_URI']

# Connect to your MongoDB cluster:
client = MongoClient(MONGODB_URI)
print("Connected to MongoDB server.")
db = client['fin-statements']
collection = db['new']

def update_labels(row):
   try:
      result = collection.update_many(
         {  
            "$and":
            [ 
               # {'lastModified': {'$exists': True}},
               {"sheets.tables.data.key": row['distinctValues']},
               # {"sheets.tables": { "$exists": "true", "$not": {"$size": 0}}},
               {"sheets.title_Fa": row['sheet']}
            ]
         # '_id': id
         },
         {
            "$set": {"sheets.$[sheet].tables.$[].data.$[element].label": row['Label']}
         },
         # array_filters=[ {"sheet.title_Fa": row['sheet']} ,{"element.key": row['distinctValues'].replace('ی', 'ي') } ] 
         array_filters=[ {"sheet.title_Fa": row['sheet']} ,{"element.key": row['distinctValues']} ] 
      )
      if result.modified_count!=0:
         # logging.info(f"No of modified: {result.modified_count}")
         pass
      else:
         logging.info(f"No document was modified for this label: {row['Label']} in sheet {row['sheet']}")

   except errors.OperationFailure as e:
      logging.info(f"Operation failed: {e}")
   except errors.PyMongoError as e:
      logging.info(f"An error occurred: {e}")
   
if __name__=="__main__":
   s3_client = get_s3_client()
   bucket_name = os.getenv('BUCKET_NAME')
   download_file(s3_client, bucket_name, 'Label.xlsx')
   label_df = pd.read_excel('Label.xlsx')
   label_df = label_df.dropna(subset=['distinctValues', 'Label'])
   label_df_fill = label_df.replace({float('nan'): None})
   label_df_fill['Label'] = label_df_fill['Label'].str.replace('ي', 'ی')

   for _, row in label_df_fill.iterrows():
      update_labels(row)
   # rows = [row for _, row in label_df_fill[label_df_fill['distinctValues'].str.contains('ی')].iterrows()]
   # rows = [row for _, row in label_df_fill.iterrows()]
   # pool = Pool(processes=8)
   # res = pool.map(update_labels, rows)
   logging.info('Finished updating labels.')
