import requests
import pickle
import os


# create data directory
os.makedirs("data", exist_ok=True)


# API url + API key + user key
url = "https://unogs-unogs-v1.p.rapidapi.com/aaapi.cgi"
headers = {
    'x-rapidapi-key': "e63c7300dcmsh6bbb616b16f9e08p11c854jsnbd5ec11c6b29",
    'x-rapidapi-host': "unogs-unogs-v1.p.rapidapi.com"
    }


# query to filter only movies
get_items_query = {"q": "-!1900,2020-!0,5-!0,10-!0-!Movie-!Any-!Any-!gt100-!{downloadable}",
                   "t": "ns", "cl": "all", "st": "adv", "ob": "Relevance", "p": "1", "sa": "or"}


# download 600 instances, here we are only downloading basic information
instances = []
for page in [str(i) for i in range(1, 2)]:  # every page has 100 instance
    get_items_query["p"] = page
    response_instances = requests.request("GET", url, headers=headers, params=get_items_query)
    print(response_instances.text)
    instances += response_instances.json()['ITEMS']

# SAVE DATA
pickle.dump(instances, open(os.path.join("data", "all_instances.pkl"), "wb"))

# collect imdb info on each movie
instances_with_no_imdb_info = []
instances_with_imdb_info = []
for i, instance in enumerate(instances):
    imdb_id = instance['imdbid']
    imdb_info_query = {"t": "getimdb",
                       "q": imdb_id}
    imdb_info = requests.request("GET", url, headers=headers, params=imdb_info_query)
    print(i, "\t", imdb_info.text)
    try:
        instance.update(imdb_info.json())
        instances_with_imdb_info.append(instance)
    except:
        instances_with_no_imdb_info.append(instance)

# SAVE DATA
pickle.dump(instances_with_no_imdb_info, open(os.path.join("data", "instances_no_imdb_info.pkl"), "wb"))
pickle.dump(instances_with_imdb_info, open(os.path.join("data", "instances_with_imdb_info.pkl"), "wb"))

# collect extra information such as: actors directors country
instances_with_no_extra_info = []
instances_with_extra_info = []
for i, instance in enumerate(instances_with_imdb_info):
    imdb_id = instance['imdbid']
    querystring = {"t": "loadvideo",
                   "q": imdb_id}
    extra_info = requests.request("GET", url, headers=headers, params=querystring)

    print(i, "\t", extra_info.text)
    try:
        instance.update(extra_info.json()['RESULT'])
        instances_with_extra_info.append(instance)
    except:
        instances_with_no_extra_info.append(instance)

# SAVE DATA
pickle.dump(instances_with_no_extra_info, open(os.path.join("data", "instances_no_imdb_and_extra_info.pkl"), "wb"))
pickle.dump(instances_with_extra_info, open(os.path.join("data", "instances_with_imdb_and_extra_info.pkl"), "wb"))
