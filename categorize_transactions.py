import traceback
import json
import pandas as pd
import csv
import logging
import sys
import datetime
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)

file_handler = logging.FileHandler(f"transaction_analysis_{datetime.now().strftime('%Y-%m-%d')}.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stdout_handler)


def get_output_column_list():
    try:
        result = ['Date','Description','Amount','Asset']
        with open('transaction_category_config.json') as f:
            data = json.load(f)
            configuratios = data.get("Configuration")
            for config in configuratios:
                if config['category'] not in result:
                    result.append(config['category'] )
                    
        result.append('Unknown')
        return result
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)

def get_keyword_category():
    try:    
        result = {}
        with open('transaction_category_config.json') as f:
            data = json.load(f)
            configuratios = data.get("Configuration")
            for config in configuratios:
                if config['keyword'] not in result.keys():
                    result[config['keyword']] = config['category']
                    
        
        return result
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)


def get_keyword_asset():
    try:    
        result = {}
        with open('transaction_asset_config.json') as f:
            data = json.load(f)
            configuratios = data.get("Configuration")
            for config in configuratios:
                if config['keyword'] not in result.keys():
                    result[config['keyword']] = config['asset']
                    
        
        return result
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)




def main():
    try:
        logger.info("Running transaction analysis...")
        df = pd.DataFrame(columns=get_output_column_list())
        
        d_keyword_category =  get_keyword_category()
        d_keyword_asset = get_keyword_asset()
     

        with open('input.csv', newline='') as f:
            reader = csv.reader(f)
            next(reader, None)
            row_count = 0

            #input file format is expected to be ['Date','Description','Amount']
            for row in reader:
                df.loc[row_count,'Date'] = row[0]
                df.loc[row_count,'Description'] = row[1]
                df.loc[row_count,'Amount'] = row[2]


                # BEGIN - Assign amount to category
                found_category = False
                for key_value in d_keyword_category.items():
                    key, value = key_value  
                    
                    # 1 is always the Description column 
                    if key in row[1]:
                        #Put amount in correct category column
                        df.loc[row_count,value] = row[2]
                        found_category = True
                
                #put amount in unknown category if you can't find it
                if not found_category:
                    df.loc[row_count,'Unknown'] = row[2]
                
                #END - Assign amount to category 

                # BEGIN - Assign amount to Asset
                for key_value in d_keyword_asset.items():
                    key, value = key_value  
                    
                    # 1 is always the Description column 
                    if key in row[1]:
                        #Set asset column with asset name
                        df.loc[row_count,'Asset'] = value

                #END - Assign amount to category 


            
                row_count +=1

            


        df.to_csv('out.csv', index=False)    


        
            

        logger.info("Completed running transaction analysis!")
    except Exception as e:
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__=="__main__":
      main()
