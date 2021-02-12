import pickle
import sys
from copy import deepcopy
from datetime import datetime
import pandas as pd
import csv
import os
# load pkl file -> raw data as downloaded from API
all_instances = pickle.load(open(os.path.join("data", "instances_with_imdb_and_extra_info.pkl"), "rb"))

# count appearances of people, take only the ones that appear at list in 5 movies
people_count = {
    "actor": {},
    "creator": {},
    "director": {}
}
for attrib_dict in all_instances:
    for people_dict in attrib_dict['people']:
        for people_type, peoples_list in people_dict.items():
            for name in peoples_list:
                people_count[people_type][name] = people_count[people_type].get(name, 0) + 1

experienced_actors = {k for k, v in people_count["actor"].items() if v >= 5}
experienced_creator = {k for k, v in people_count["director"].items() if v >= 3}

all_genres = {}
for attrib_dict in all_instances:
    for genre in attrib_dict['genre'].split():
        all_genres[genre.replace(',', '').strip()] = 0


def get_awards_and_nominations(award_str):
    awards = [a for a in award_str.split('&') if "win" in a]
    awards = awards[0] if awards else '0'
    nominations = [a for a in award_str.split('&') if "nomination" in a]
    nominations = nominations[0] if nominations else '0'

    awards = sum([int(i) for i in awards.split() if i.isdigit()])
    nominations = sum([int(i) for i in nominations.split() if i.isdigit()])
    return awards, nominations


# extract features out of data dict
def extract_instance(attrib_dict):
    num_experienced_actor = 0
    num_experienced_creator = 0

    # collect actor and directors
    for people_dict in attrib_dict['people']:
        for people_type, peoples_list in people_dict.items():
            for name in peoples_list:
                if people_type == 'actor' and name in experienced_actors:
                    num_experienced_actor += 1
                elif people_type == 'creator' and name in experienced_creator:
                    num_experienced_creator += 1

    # collect genres
    instance_genres = deepcopy(all_genres)
    for genre in attrib_dict['genre'].split():
        instance_genres[genre.replace(',', '').strip()] = 1

    # missing attributes
    if any([attrib_dict[k] == '' or attrib_dict[k] == 'N/A' or attrib_dict[k] == 'notfound' or 'None' in attrib_dict[k] for k in
            ['netflixid', 'title', 'released', 'runtime', 'awards', 'language', 'imdbinfo', 'rating']]):
        return None, None

    awards, nominations = get_awards_and_nominations(attrib_dict['awards'])
    instance = {
        'netflixid': attrib_dict['netflixid'],
        'title': attrib_dict['title'],
        'released_year': datetime.strptime(attrib_dict['released'], "%d %b %Y").year,
        'released_month': datetime.strptime(attrib_dict['released'], "%d %b %Y").month,
        'runtime': int(attrib_dict['runtime'].replace('min', '')),
        'awards_win': awards,
        'awards_nominations': nominations,
        'language': attrib_dict['language'],
        'country': attrib_dict['imdbinfo']['country'],
        'num_experienced_actors': num_experienced_actor,
        'num_experienced_creators':  num_experienced_creator
    }

    instance.update(instance_genres)
    instance_label = {'netflixid': attrib_dict['netflixid'],
                      'rating': float(attrib_dict['rating'])}
    return instance, instance_label


# collect instance attributes and rating
instances_features = []
instances_labels = []
for attrib_dict in all_instances:
    instance, instance_label = extract_instance(attrib_dict)

    # skip instances with missing attributes
    if instance is None:
        print(f"{attrib_dict['netflixid']} is missing an attribute")
        continue

    instances_features.append(instance)
    instances_labels.append(instance_label)

# build features csv file
headers = ['netflixid', 'title', 'released_year', 'released_month', 'runtime', 'awards_win', 'awards_nominations',
           'language', 'country', 'num_experienced_actors', 'num_experienced_creators'] + sorted(list(all_genres.keys()))
table = [headers]
for instance in instances_features:
    table.append([instance[col_header] for col_header in headers])
with open(os.path.join("data", "netflix_features.csv"), "wt", newline='') as f:
    writer = csv.writer(f)
    writer.writerows(table)

# build label csv file
labels_df = pd.DataFrame(instances_labels)
labels_df.to_csv(os.path.join("data", "netflix_labels.csv"), index=False)
