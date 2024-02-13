from ast import Pass
import json
from queue import Empty
import sys
from turtle import position
import uuid
import os
from firebase_admin import db
from google.cloud.firestore_v1 import FieldFilter
from google.cloud import firestore

from firebase import authenticate

# authenticate fucntion from firebase.py
db = authenticate()

# reference to collection in firebase
teams_ref = db.collection("nfl-teams")

# function that will process user queries


def process_queries():
    while True:
        global used_of
        global valid_operand
        valid_operand = True
        used_of = False
        # Collection of teams
        query = teams_ref
        # Check input
        input = validate_input()
        # if input is help, run the help function
        if input == "help":
            print_help()
            used_of = True
        elif input == 0:
            Pass
        else:
            # if input is valid, run the query results function with the input passed in
            if valid_operand:
                query_results(input)

# function to query user input


def query_results(input):
    global used_of
    used_of = False
    query = teams_ref
    # first string will be input, second will be operand, third will be subject user is querying
    parameters = input[0]
    operands = input[1]
    subjects = input[2]
    # if the operand if 'of' run the get_of function with parameter and subject
    for operand in operands:
        if operand == "of":
            used_of = True
            get_of(parameters, subjects)
    # if 'of' is not used and parameter is 'rank' convert subject to int
    if not used_of:
        if parameters[0] == "rank":
            try:
                subjects[0] = int(subjects[0])
            except:
                Pass
            # query rank from top_100 sub-collection
            query1 = db.collection_group("top_100").where(
                filter=FieldFilter(parameters[0], operands[0], subjects[0]))
            docs_sub = query1.stream()
            docs1 = []
            for doc in docs_sub:
                path = doc.reference.path
                sub_path = path.split('/')[1]
                if sub_path not in docs1:
                    docs1.append(sub_path)
        # if parameter is 'name' or 'position' the other fields in the sub-collection, the query them, they are different as they are strings
        elif parameters[0] == "name" or parameters[0] == "position":
            query1 = db.collection_group("top_100").where(
                filter=FieldFilter(parameters[0], operands[0], subjects[0]))
            docs_sub = query1.stream()
            docs1 = []
            for doc in docs_sub:
                path = doc.reference.path
                sub_path = path.split('/')[1]
                if sub_path not in docs1:
                    docs1.append(sub_path)
        else:
            # again convert to int if last_championship is the parameter
            if parameters[0] == "last_championship":
                try:
                    subjects[0] = int(subjects[0])
                except:
                    Pass
            query1 = query.where(filter=FieldFilter(
                parameters[0], operands[0], subjects[0]))
            docs_temp = query1.stream()
            docs1 = []
            for doc in docs_temp:
                team_data = doc.to_dict()
                docs1.append(team_data.get("name"))

        # handling for compound queries, with same or similar for different
        if len(parameters) > 1:
            if parameters[1] == "rank":
                subjects[1] = int(subjects[1])
                query2 = db.collection_group("top_100").where(
                    filter=FieldFilter(parameters[1], operands[1], subjects[1]))
                docs_sub2 = query2.stream()
                docs2 = []
                for doc in docs_sub2:
                    path = doc.reference.path
                    sub_path = path.split('/')[1]
                    if sub_path not in docs2:
                        docs2.append(sub_path)

            # compound query handling for name and position
            elif parameters[1] == "name" or parameters[1] == "position":
                query2 = db.collection_group("top_100").where(
                    filter=FieldFilter(parameters[1], operands[1], subjects[1]))
                docs_sub2 = query2.stream()
                docs2 = []
                for doc in docs_sub2:
                    path = doc.reference.path
                    sub_path = path.split('/')[1]
                    if sub_path not in docs2:
                        docs2.append(sub_path)
            # last_championship compound handling
            else:
                if parameters[1] == "last_championship":
                    subjects[1] = int(subjects[1])
                query2 = query.where(filter=FieldFilter(
                    parameters[1], operands[1], subjects[1]))
                docs_temp = query2.stream()
                docs2 = []
                for doc in docs_temp:
                    team_data = doc.to_dict()
                    docs2.append(team_data.get("name"))

            docs1_set = set(docs1)
            docs2_set = set(docs2)
            # Find the intersection of the two queries
            common_docs = docs1_set.intersection(docs2_set)
            combined = list(common_docs)
        else:
            combined = docs1
        if len(combined) == 0:
            print("No Information")
        else:
            result = ", ".join(combined)
            if result is None:
                print("No Information")
            else:
                print(result)

# input validation function


def validate_input():
    subcollections = ["rank", "name", "position"]
    contains_of = False
    global valid_operand
    used_help = False
    used_sub = False
    query_input = input(">")
    # quit out of query if user inputs 'quit'
    if query_input.lower() == "quit":
        exit()
    # print help fucntion if user inputs 'help'
    if query_input.lower() == "help":
        used_help = True
        valid_operand = False
    if not used_help:
        # do not let user input more than two conditions if and is used
        conditions = query_input.split(" and ")
        if len(conditions) > 2:
            print("Please limit queries to two conditions. Enter 'help' for more info.")
        else:
            for condition in conditions:
                if "of" in condition:
                    contains_of = True
            if contains_of:
                if len(conditions) > 1:
                    valid_operand = False
                    # if and is not used, only let user use one condition
                    print("Invalid query. Enter 'help' for more info.")
                else:
                    parameters = []
                    operands = []
                    subjects = []
                    # append conditions to parameters, operands, and subjects respectively.
                    for condition in conditions:
                        values = get_operand(condition)
                        if values == 0:
                            return 0
                        else:
                            parameters.append(values[0])
                            operands.append(values[1])
                            subjects.append(values[2])
                    filters = []
                    filters.append(parameters)
                    filters.append(operands)
                    filters.append(subjects)
                    return filters
            else:
                parameters = []
                operands = []
                subjects = []
                for condition in conditions:
                    values = get_operand(condition)
                    if values == 0:
                        return 0
                    else:
                        parameters.append(values[0])
                        operands.append(values[1])
                        subjects.append(values[2])
                filters = []
                filters.append(parameters)
                filters.append(operands)
                filters.append(subjects)
                return filters
    else:
        return "help"

# function to get operands


def get_operand(condition):
    global valid_operand
    values = []
    extra_space = False

    if (" == " in condition):

        parameter, subject = condition.split(" == ")
        operand = "=="
        if " " in parameter:
            extra_space = True

    elif (" >= " in condition):

        parameter, subject = condition.split(" >= ")
        operand = ">="
        if " " in parameter:
            extra_space = True

    elif (" <= " in condition):

        parameter, subject = condition.split(" <= ")
        operand = "<="
        if " " in parameter:
            extra_space = True

    elif (" > " in condition):

        parameter, subject = condition.split(" > ")
        operand = ">"
        if " " in parameter:
            extra_space = True

    elif (" < " in condition):

        parameter, subject = condition.split(" < ")
        operand = "<"
        if " " in parameter:
            extra_space = True

    elif (" of " in condition):
        parameter, subject = condition.split(" of ")
        operand = "of"
        if " " in parameter:
            extra_space = True

    else:  # Invalid operand passed
        print("Inavlid Query. Please Try again, or enter 'help' for more info")
        valid_operand = False
        # Set paramater, subject, and operand to dummy variables that won't get used
        parameter = "nothing"
        subject = "nothing"
        operand = ">"
    if extra_space:
        print("Inavlid Query. Please Try again, or enter 'help' for more info")
        valid_operand = False
        # Set paramater, subject, and operand to dummy variables that won't get used
        values = 0
    else:
        values.append(parameter)
        values.append(operand)
        values.append(subject)
    return values

# handling for unique 'of' operand


def get_of(parameters, subjects):
    query = teams_ref
    query = query.where(filter=FieldFilter(
        'name', '==', subjects[0]))
    docs = query.stream()
    # 'of' can retrieve data from sub-collection
    if parameters[0] == "top_100":
        for doc in docs:
            team_data = doc.to_dict()
            name = team_data.get("name")
            team_ref = teams_ref.document(name)
            data = team_ref.collection("top_100")
            data = data.stream()
            for i in data:
                print("")
                print("Name:", i.get("name"))
                print("Position:", i.get("position"))
                print("Rank:", i.get("rank"))
    else:
        for doc in docs:
            team_data = doc.to_dict()
            if team_data.get(parameters[0]) is None:
                # tell user if no information is available
                print("No Information")
            else:
                print(team_data.get(parameters[0]))

# help function, prints out premade example queries


def print_help():
    help_txt = "HELP"
    intro_txt = "Welcome to the NFL data query! You can ask questions about NFL teams. Below you can find\
 a list of valid parameter,\n operand, and subject inputs as well as some examples. This program has more functionality\
 than is given in these\n examples, but they are made to give you an idea of the format that it accepts."
    input1_txt = "Valid Parameter Inputs"
    param_txt = "'coach', 'city', 'name', 'owner', 'top_100'"
    input2_txt = "Valid Operand Inputs"
    operand_txt = "'==', '>', '<', '>=', '<=', 'of'"
    input3_txt = "Valid Subject Inputs"
    subject_txt = "'New York Jets', 'New York', '1990', 'Glendale', 'Bill Belicheck', etc.."
    input4_txt = "Example Valid Inputs"
    example1_txt = "'city == New York', 'last_championship >= 1994', 'top_100 of New York Jets', 'coach of New York Jets'"
    example2_txt = "'city == Tampa Bay and last_championship > 1994', 'last_championship > 1990 and last_championship < 2013'"
    example3_txt = "'rank > 50 and city == New York', 'position == CB', 'name == Sauce Gardner'"

    help_str = help_txt.center(120, "=")
    intro_str = intro_txt.center(120)
    input1_str = input1_txt.center(120, "=")
    param_str = param_txt.center(120)
    input2_str = input2_txt.center(120, "=")
    operand_str = operand_txt.center(120)
    input3_str = input3_txt.center(120, "=")
    subject_str = subject_txt.center(120)
    input4_str = input4_txt.center(120, "=")
    example1_str = example1_txt.center(120)
    example2_str = example2_txt.center(120)
    example3_str = example3_txt.center(120)

    print(help_str)
    print("")
    print(intro_str)
    print("")
    print(input1_str)
    print("")
    print(param_str)
    print("")
    print(input2_str)
    print("")
    print(operand_str)
    print("")
    print(input3_str)
    print("")
    print(subject_str)
    print("")
    print(input4_str)
    print("")
    print(example1_str)
    print("")
    print(example2_str)
    print("")
    print(example3_str)
    print("")


process_queries()
