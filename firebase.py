import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import uuid


def authenticate():
    cred = credentials.Certificate(
        'cs3050-warmup-project-firebase-adminsdk.json')

    app = firebase_admin.initialize_app(cred)

    db = firestore.client()
    return db
