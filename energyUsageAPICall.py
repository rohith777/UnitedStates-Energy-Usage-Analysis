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
    output_file = "/Users/narra/Documents/energyUsage/Data/energyUsageData.json" 
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
    categories = ['40549','40563','40562','40560','40855','2804525','40553','2804581','2804527','40359','40370','40530','40293','40547','40543','40859','2804530','40534','2804571','45563','40344','40372','40484','40308','40520','40515','40856','2804476','40491','2804488','45565','40336','40371','40565','40282','40587','40583','2804519','40569','2804522','40373']
    for category in categories:
        series_ids = fetch_series_ids(get_category_details(category).json())
        json_file = write_to_file(series_ids)
    print("Output JSON Data Present in : {0}".format(json_file))