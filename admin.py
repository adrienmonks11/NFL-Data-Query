import firebase_admin
from firebase_admin import firestore
import json
import sys
import uuid
import firebase
import os
import subprocess

from firebase import authenticate

db = authenticate()

# obtain json file from cmd
json_import = sys.argv[1]


class Team:
    def __init__(self, name, city, owner, coach, last_championship=None):
        self.name = name
        self.city = city
        self.owner = owner
        self.coach = coach
        self.last_championship = last_championship

    def from_dict(source):
        team = Team(source["name"], source["city"],
                    source["owner"], source["coach"], source["last_championship"])
        return team

    def to_dict(self):
        team = {"name": self.name, "city": self.city,
                "owner": self.owner, "coach": self.coach, "last_championship": self.last_championship}
        return team

    def __repr__(self):
        return f"Team(\
                name={self.name}, \
                city={self.city}, \
                owner={self.owner}, \
                coach={self.coach}, \
                last_championship = {self.last_championship}\
            )"


with open(json_import, 'r') as json_file:
    data = json.load(json_file)


def delete_collection(coll_ref, batch_size):
    docs = coll_ref.list_documents(page_size=batch_size)
    deleted = 0

    for doc in docs:
        doc.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)


delete_collection(db.collection('nfl-teams'), 100)
#delete_collection(db.collection("top_100"), 100)


# Reference to teams
teams_ref = db.collection("nfl-teams")

for item in data:
    name = item["name"]
    city = item["city"]
    owner = item["owner"]
    coach = item["coach"]
    top_100 = item["top_100"]
    if "last_championship" in item:
        last_championship = item["last_championship"]
    else:
        last_championship = None

    team_ref = teams_ref.document(name)
    delete_collection(team_ref.collection("top_100"), 10)
    team_ref.set(Team(name, city, owner, coach, last_championship).to_dict())

    top_100_collection = team_ref.collection("top_100")

    for player in top_100:
        top_100_collection.add(player)
