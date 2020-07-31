import requests
import json

def get_category_details(category):
    url = f"https://api.eia.gov/category/?api_key=08c29a9574883bcf73530c1bf8d4fcc3&category_id={category}"
    response = requests.get(url)
    if response.status_code != 200:
        if response.status_code == 404:
            print("Category not found for")
            return None
        else:
            raise Exception("API Hit Failed ")
    return response

def write_to_file(series_ids):
    output_file = "/Users/narra/Documents/energyUsage/Data/energyProductionData.json" 
    with open(output_file, 'a',encoding='utf-8') as f:
        for i in series_ids:
            print(i)
            url = f"http://api.eia.gov/series/?api_key=08c29a9574883bcf73530c1bf8d4fcc3&series_id={i}"
            response = requests.get(url)
            if response:
                    f.write(json.dumps(response.json()))
                    f.write("\n")
    return output_file

def fetch_series_ids(data):
    data = data["category"]
    series_ids = []
    for i in data["childseries"]:
        series_ids.append(i["series_id"])
    return series_ids

if __name__ == "__main__":
    categories = ['40812','40815','40824','40426','40207']
    
    for category in categories:
        series_ids = fetch_series_ids(get_category_details(category).json())
        json_file = write_to_file(series_ids)
    print("Output JSON Data Present in : {0}".format(json_file))