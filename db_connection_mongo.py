#-------------------------------------------------------------------------
# AUTHOR: Aye Mon
# FILENAME: db_connection_mongo.py
# SPECIFICATION: using pymongo to connect to the database
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient
import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient( host = DB_HOST, port = DB_PORT)
        db = client[DB_NAME]

        return db
    except:
        print("Database not connected sucessfully!")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    termFreq = {}
    terms = docText.lower().split(" ")
    for term in terms:
        if term in termFreq:
            termFreq[term] += 1
        else:
            termFreq[term] = 1
    # create a list of dictionaries to include term objects.
    term_list = []
    for term, count in termFreq.items():
        term_list.append({"term": term, "count": count})

    #Producing a final document as a dictionary including all the required document fields
    documents = {
        "id": docId,
        "text": docText,
        "title": docTitle,
        "date": docDate,
        "category": docCat,
        "term_list": term_list
    }


    # Insert the document
    col.insert_one(documents)

def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    index = {}

    for doc in col.find():
        docTitle = doc["title"]
        for term_obj in doc["term_list"]:
            term = term_obj["term"]
            count = term_obj["count"]
            
            if term in index:
                if docTitle in index[term]:
                    index[term][docTitle] += count
                else:
                    index[term][docTitle] = count
            else:
                index[term] = {docTitle: count}

    return index
