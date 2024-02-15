#!/usr/bin/env python
# coding: utf-8

# # Add column to existing .csv

# In[18]:


import pandas as pd
import requests
import itertools
from tqdm import tqdm


# In[2]:


def fetch_news_id(tag_ids: list, file="hk01_fulloutput.csv":str):
    df = pd.read_csv(file)
    data = []
    for element in tag_ids:
        offset = "<OFFSET>"
        while True:
            hk01_api_article_list = f"https://web-data.api.hk01.com/v2/feed/tag/{element}?offset={offset}"
            response = requests.get(hk01_api_article_list)
            
            # Check for successful response
            if response.status_code == 200:
                json_data = response.json()
                try:
                    # Update offset for pagination
                    new_offset = str(json_data["nextOffset"])
                    if new_offset:
                        offset = new_offset
                        new_data = [i["data"].get("articleId") 
                                    for i in json_data.get("items", [])]
                        data.append(new_data)
                except KeyError:
                    # Break the loop if no further data is available
                    break
            else:
                print(f"Failed to fetch data for tag {element}. HTTP Status Code: {response.status_code}")
                break

    # Flatten the list of data and create DataFrame
    flatten_data = list(itertools.chain(*data))
    difference = list(set(flatten_data) - set(list(df.articleId)))
    return difference


# In[19]:


def extracting_news_from_articleid(news_ids: list):
    news_data = []  # Initialize an empty list to store news details
    for news_id in tqdm(news_ids, desc="Extracting Article content"):
        news_link = f"https://web-data.api.hk01.com/v2/page/article/{news_id}"
        response = requests.get(news_link)  # Send a request to the API
        if response.status_code == 200:  # Check if the request was successful
            try:
                news_json = response.json()  # Parse the JSON data from the response
                # Extract required details and append them to the news_data list
                news_data.append([
                    news_id,
                    news_json["article"]["publishUrl"],
                    news_json["article"]["description"],
                    news_json["article"]["publishTime"]
                ])
            except KeyError:  # Handle missing keys in the JSON data
                # Optionally, you can log the error or handle it differently
                print(f"KeyError encountered for news_id: {news_id}")
                continue  # Continue to the next iteration instead of breaking the loop
    df = pd.DataFrame(news_data, columns=["articleId", 'publishUrl', 'description', "publishTime"])
    df["publishTime"] = pd.to_datetime(df["publishTime"], unit="s")
    df.drop_duplicates(subset=["articleId"])
    return df


# In[35]:


def output_file(df, output_filename):
    try:
        # Ensure the filename ends with '.csv'
        if not output_filename.endswith('.csv'):
            output_filename += '.csv'

        # Exporting DataFrame to CSV
        df.to_csv(output_filename, encoding="utf-8-sig", index=False)
        print(f"File '{output_filename}' has been successfully saved.")

    except IOError as e:
        print(f"Error occurred while saving the file: {e}")


# In[36]:


def run_this(tag_id: list, input_filename: str, output_filename: str):
    df = pd.read_csv(input_filename)
    current_news_list = fetch_news_id([347,348,15217,25459],input_filename)
    new_df = extracting_news_from_articleid(current_news_list)
    df_full = pd.concat([df,new_df])
    output_file(df_full,output_filename)


# In[37]:


run_this([347,348,15217,25459],"hk01_fulloutput.csv","hk01_fulloutput_addition.csv")


# In[ ]:





# In[ ]:




