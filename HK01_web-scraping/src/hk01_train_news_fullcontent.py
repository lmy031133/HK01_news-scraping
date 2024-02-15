# ## URL to Reference
# https://pastebin.com/J3HARz0g

# # HK01 News Scraper
# This notebook outlines the process of scraping news articles from HK01 based on different topic IDs. We extract key details such as article ID, URL, description, publish time, and full content. This script is designed to be a robust and efficient way to gather news data for analysis.


import requests  # To make HTTP requests to HK01
import pandas as pd  # For data manipulation and analysis
import json  # To parse JSON responses
import itertools  # For efficient looping
from tqdm import tqdm  # For displaying progress bars


def fetch_news_data(tag_ids: list):
    """
    Fetches news data from HK01 for given tag IDs and creates a DataFrame.

    Each tag ID corresponds to a specific news category. This function makes HTTP requests
    to the HK01 API, retrieves news articles for each tag ID, and compiles the data into
    a structured DataFrame.

    Parameters:
    - tag_ids (list of int): A list of tag IDs for which to fetch news articles.
      Example tag IDs: 港鐵系統失靈 is 347, 港鐵班次延誤 is 348.

    Returns:
    - pandas.DataFrame: A DataFrame containing the following columns:
      ['articleId', 'publishUrl', 'description', 'publishTime'].
      Each row corresponds to a unique news article.
    
    Notes:
    - The function iterates through each tag ID and makes paginated requests to the API.
    - Data for each article includes its ID, URL, description, and publish time.
    - Duplicate articles (based on 'articleId') are dropped to ensure uniqueness.
    """

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
                        new_data = [[i["data"].get("articleId"), 
                                     i["data"].get("publishUrl"), 
                                     i["data"].get("description"), 
                                     i["data"].get("publishTime")] 
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
    df = pd.DataFrame(flatten_data, columns=["articleId", 'publishUrl', 'description', "publishTime"])
    
    # Remove duplicate entries based on articleId
    return df.drop_duplicates(subset=["articleId"])

def add_article_summary_and_format_time(df):
    """
    Enhances the DataFrame by adding a summary for each article and formatting the publish time.

    This function iterates through each article ID in the DataFrame, fetches its summary (teaser)
    from the HK01 API, and appends it to the DataFrame. It also formats the publish time of each
    article to a readable datetime format.

    Parameters:
    - df (pandas.DataFrame): A DataFrame containing at least 'articleId' and 'publishTime' columns.

    Returns:
    - pandas.DataFrame: The enhanced DataFrame with an added 'Summary' column and formatted 'publishTime'.

    Notes:
    - The function retrieves the teaser of each article using its 'articleId'.
    - The 'publishTime' column is converted from Unix timestamp to a datetime object for readability.
    """

    # Prepare a list to store the summaries
    summaries = []

    # Iterate through each article ID to fetch its summary
    for article_id in df['articleId']:
        try:
            hk01_article_detail = f"https://web-data.api.hk01.com/v2/page/article/{article_id}"
            response = requests.get(hk01_article_detail)
            
            if response.status_code == 200:
                detail_dict = response.json()
                teaser = detail_dict["article"].get("teaser", "")
                summaries.append(teaser)
            else:
                print(f"Failed to fetch details for article {article_id}. HTTP Status Code: {response.status_code}")
                summaries.append("")  # Append an empty string in case of failure

        except requests.RequestException as e:
            print(f"Error occurred while fetching article {article_id}: {e}")
            summaries.append("")  # Append an empty string in case of error

    # Adding the summaries to the DataFrame
    df["Summary"] = summaries

    # Formatting the 'publishTime' column
    df["publishTime"] = pd.to_datetime(df["publishTime"], unit="s")

    return df


def extract_and_format_text(df):
    """
    Extracts and formats the content of each article in the DataFrame.

    Iterates over each article ID in the DataFrame, makes a request to the HK01 API to fetch
    the article's content, and formats it. The formatted content is then appended to the DataFrame.

    Parameters:
    - df (pandas.DataFrame): A DataFrame containing the 'articleId' column.

    Returns:
    - pandas.DataFrame: The enhanced DataFrame with an added 'allContent' column containing
      the formatted text of each article.

    Notes:
    - The function uses 'tqdm' to display a progress bar for the extraction process.
    - The content is formatted by appending different types of text blocks, including bold text.
    - In case of an HTTP error or other request issues, an error message is appended.
    """

    # List to store the formatted text of each article
    article_list = []
    
    # Progress bar for extraction process
    for article_id in tqdm(df['articleId'], desc="Extracting Article content"):
        formatted_text = ""

        try:
            url = f"https://web-data.api.hk01.com/v2/page/article/{article_id}"
            response = requests.get(url)
            response.raise_for_status()  # Raises an error for HTTP error codes

            article_data = response.json()

            # Parsing and formatting the article content
            for block in article_data["article"]["blocks"]:
                if block["blockType"] == "text":
                    for token_list in block.get("htmlTokens", []):
                        for token in token_list:
                            if token["type"] == "boldText":
                                formatted_text += token["content"] + ":"
                            elif token["type"] == "text":
                                formatted_text += token.get("content", "")
                        formatted_text += "\n"
            
            article_list.append(formatted_text)
        except requests.RequestException as e:
            formatted_text = f"Error fetching data: {e}"
            article_list.append(formatted_text)

    # Appending the formatted content to the DataFrame
    df["allContent"] = article_list
    return df


def output(df, filename):
    """
    Exports the given DataFrame to a CSV file.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be exported.
    - filename (str): The name of the output file. The function will add '.csv' extension
      if not already present.

    Returns:
    - None: The function writes the DataFrame to a CSV file and returns nothing.

    Notes:
    - The function uses 'utf-8-sig' encoding, which is suitable for CSV files containing
      special characters and ensures compatibility with most CSV readers, including Excel.
    - The index of the DataFrame is not included in the output file.
    - In case of any IOError, an appropriate message is printed.
    """

    try:
        # Ensure the filename ends with '.csv'
        if not filename.endswith('.csv'):
            filename += '.csv'

        # Exporting DataFrame to CSV
        df.to_csv(filename, encoding="utf-8-sig", index=False)
        print(f"File '{filename}' has been successfully saved.")

    except IOError as e:
        print(f"Error occurred while saving the file: {e}")


def run_this(tag_id:list,filename:str):
    df = fetch_news_data(tag_id)
    df = add_article_summary_and_format_time(df)
    df = extract_and_format_text(df)
    output(df,filename)




