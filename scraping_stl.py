import requests
import re
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm
import numpy as np

###########################
### Resources to scrape

# URLs of global ranking pages (to get bib numbers), and individual runners pages (to get time splits)
urls = {2024 : ["https://saintelyon.livetrail.net/classement.php?course=82km&cat=scratch", "https://saintelyon.livetrail.net/coureur.php?rech="],
        2023 : ["https://livetrail.net/histo/saintelyon_2023/classement.php?course=78km&cat=scratch", "https://livetrail.net/histo/saintelyon_2023/coureur.php?rech="],
        2022 : ["https://livetrail.net/histo/saintelyon_2022/classement.php?course=78km&cat=scratch", "https://livetrail.net/histo/saintelyon_2022/coureur.php?rech="],
        2021 : ["https://livetrail.net/histo/saintelyon_2021/classement.php?course=78km&cat=scratch", "https://livetrail.net/histo/saintelyon_2021/coureur.php?rech="],
        2019 : ["https://livetrail.net/histo/saintelyon_2019/classement.php?course=76km&cat=scratch", "https://livetrail.net/histo/saintelyon_2019/coureur.php?rech="],
        2018 : ["https://livetrail.net/histo/saintelyon_2018/classement.php?course=81km&cat=scratch", "https://livetrail.net/histo/saintelyon_2018/coureur.php?rech="],
        2016 : ["https://livetrail.net/histo/saintelyon_2016/classement.php?course=72km&cat=scratch", "https://livetrail.net/histo/saintelyon_2016/coureur.php?rech="],
        2015 : ["https://livetrail.net/histo/saintelyon_2015/classement.php?course=75km&cat=scratch", "https://livetrail.net/histo/saintelyon_2015/coureur.php?rech="],
        2014 : ["https://livetrail.net/histo/saintelyon_2014/classement.php?course=75km&cat=scratch", "https://livetrail.net/histo/saintelyon_2014/coureur.php?rech="],
        2013 : ["https://livetrail.net/histo/saintelyon_2013/classement.php?course=75km&cat=scratch", "https://livetrail.net/histo/saintelyon_2013/coureur.php?rech="],
        2013 : ["https://livetrail.net/histo/saintelyon_2013/classement.php?course=75km&cat=scratch", "https://livetrail.net/histo/saintelyon_2013/coureur.php?rech="]}
url2017 = "https://livetrail.net/histo/saintelyon_2017/classement.php?course=72km&cat=scratch"

# Notes : 
# There was no 2020 edition (covid)
# Individual pages for the 2017 edition don't work, so it will be handled separately

###########################
### Helper functions

def extract_bib_numbers(url:str) -> list[int]:
    '''Function to extract all bib numbers from a general ranking page.

    Args : 
        url (str) : string URL of a general ranking page
    Returns :
        bib_numbers (list[int]) : list of all bib numbers found
    '''
    response = requests.get(url)
    if response.status_code == 200:
        content = response.text
        bib_numbers = re.findall(r'doss="(\d+)"', content)
        return bib_numbers
    else:
        print("Failed to retrieve the page")
        return []

def extract_checkpoints(url, year):
    '''Function to extract information about all checkpoints for a given year :
    distance, relative elevation, name. Ignores irrelevant checkpoints from 2023 and 2024.

    Args : 
        url (str) : string URL of any personal runner page
        year (int) : race year
    Returns :
        checkpoints_info (dict) : dictionary containing relevant checkpoint info
    '''    
    response = requests.get(url)
    root = ET.fromstring(response.text)
    checkpoints = root.findall(".//pts/pt")
    checkpoints_info = {"Year" : year}
    i = 0
    for cp in checkpoints:
        if cp.attrib["n"] == "Animation 500m" or cp.attrib["n"] == "KM BV SPORT" : 
            continue
        checkpoints_info[f"Pt{i} name"] = cp.attrib["n"]
        checkpoints_info[f"Pt{i} distance"] = cp.attrib["km"]
        checkpoints_info[f"Pt{i} height"] = cp.attrib["a"]
        checkpoints_info[f"Pt{i} total elevation"] = cp.attrib["d"]
        i += 1
    return checkpoints_info


###########################
### Main scraping loop

# Initialising the two main storages we'll convert to csv later :
data = []
checkpoint_data = []

for year in urls : 
    base_ranking_url = urls[year][0]
    base_runner_url = urls[year][1]

    # Extract all bib numbers
    bib_numbers = extract_bib_numbers(base_ranking_url)

    # Extract checkpoint info
    checkpoint_data.append(extract_checkpoints(base_runner_url + bib_numbers[0], year))
    
    # Loop through each runner's page to extract details (using tqdm to show a progress bar)
    for bib in tqdm(bib_numbers, desc=f"Processing {year} results"):
        runner_url = base_runner_url + bib
        response = requests.get(runner_url)
        response.raise_for_status()
        root = ET.fromstring(response.text) # Parse XML

        # Personal information
        identite = root.find(".//identite")
        name = identite.attrib["nom"]
        first_name = identite.attrib["prenom"] if 'prenom' in identite.attrib.keys() else None # Someone doesn't have a name in 2013 data
        sex = identite.attrib["sx"]
        category = identite.attrib["cat"]
        club = identite.attrib.get("club", "")
        nationality = identite.attrib["nat"]

        # Rankings
        state = root.find(".//state")
        global_rank = state.attrib["clt"]
        category_rank = state.attrib["cltcat"] if "cltcat" in state.attrib.keys() else None # Safeguard for anonymised bib numbers
        gender_rank = state.attrib["cltsx"] if "cltsx" in state.attrib.keys() else None

        # Checkpoint names
        checkpoints = root.findall(".//pts/pt")
        checkpoint_dict = {cp.attrib["idpt"]: cp.attrib["n"] for cp in checkpoints}
        checkpoints_ids = [cp.attrib["idpt"] for cp in checkpoints]

        # Time splits and checkpoint rankings
        splits = root.findall(".//pass/e")
        cptimes, cpranks = [], []
        useless = ["Animation 500m", "KM BV SPORT"]
        i = 0
        for id in checkpoints_ids:
            if (checkpoint_dict[id] in useless) and (checkpoint_dict[splits[i].attrib["idpt"]] in useless) : # Ignore useless checkpoints
                i += 1
                continue
            elif checkpoint_dict[id] in useless : # rare case where the runner has no time recorded at a useless checkpoint
                continue

            if id == splits[i].attrib["idpt"]:
                cptimes.append(splits[i].attrib["tps"])
                cpranks.append(splits[i].attrib.get("clt", "-"))
                i += 1
            else : # No time recorded at this checkpoint
                cptimes.append(np.nan) # Still adding a NaN value to keep them in order
                cpranks.append(np.nan)

        # Previous achievements
        past_races = root.findall(".//palm/e")
        achievements = [
            {
                "Year": race.attrib["year"],
                "Race": race.attrib["race"],
                "Position": race.attrib["pos"],
                "Time": race.attrib["tps"],
                "Distance": race.attrib["dist"],
                "Elevation Gain": race.attrib["deniv"] if "deniv" in race.attrib.keys() else None # Before 2016 : no elevation data on past races
            }
            for race in past_races
        ]

        # UTMB Index if present
        palm = root.find(".//palm")
        index = palm.attrib.get('cote') if palm is not None else None

        # Append data to the list
        data.append({
            "Year": year,
            "Bib Number": bib,
            "Last Name": name,
            "First Name": first_name,
            "Sex": sex,
            "Category": category,
            "Club": club,
            "Nationality": nationality,
            "Global Rank": global_rank,
            "Category Rank": category_rank,
            "Gender Rank": gender_rank,
            "UTMB Index": index,
            "Pt0 time" : cptimes[0],
            "Pt0 rank" : cpranks[0],
            "Pt1 time" : cptimes[1],
            "Pt1 rank" : cpranks[1],
            "Pt2 time" : cptimes[2],
            "Pt2 rank" : cpranks[2],
            "Pt3 time" : cptimes[3],
            "Pt3 rank" : cpranks[3],
            "Pt4 time" : cptimes[4],
            "Pt4 rank" : cpranks[4],
            "Pt5 time" : cptimes[5],
            "Pt5 rank" : cpranks[5],
            "Pt6 time" : cptimes[6],
            "Pt6 rank" : cpranks[6],
            "Past Achievements": achievements
        })
    
    # Saving the data after each year in case of a problem
    df = pd.DataFrame(data)
    df.to_csv("Saintelyon_Results_new.csv", index=False, encoding="utf-8")
    checkpoint_df = pd.DataFrame(checkpoint_data)
    checkpoint_df.to_csv("Saintelyon_checkpoints.csv", index=False, encoding="utf-8")


###########################
### 2017 edition loop

response = requests.get(url2017)
root = ET.fromstring(response.content)

# Loop through the general rankings and get as much info as possible
for runner in tqdm(root.findall(".//classement/c"), desc=f"Processing 2017 results"):
    data.append({
            "Year": 2017,
            "Bib Number": runner.attrib.get('doss'),
            "Last Name": runner.attrib.get('nom'),
            "First Name": runner.attrib.get('prenom'),
            "Sex": runner.attrib.get('sx'),
            "Category": runner.attrib.get('cat'),
            "Club": runner.attrib.get('club'),
            "Nationality": runner.attrib.get('pays'),
            "Global Rank": runner.attrib.get('class'),
            "Category Rank": runner.attrib.get('classcat'),
            "UTMB Index": runner.attrib.get('index'),
            "Pt6 time": runner.attrib.get('tps'),
            "Pt6 rank": runner.attrib.get('class')
        })


# Final save
df = pd.DataFrame(data)
df.to_csv("Saintelyon_Results.csv", index=False, encoding="utf-8")
checkpoint_df = pd.DataFrame(checkpoint_data)
checkpoint_df.to_csv("Saintelyon_checkpoints.csv", index=False, encoding="utf-8")

print("Data extraction complete! CSV saved as 'Saintelyon_Results.csv' and 'Saintelyon_checkpoints.csv.")