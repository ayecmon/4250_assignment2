#-------------------------------------------------------------------------
# AUTHOR: Aye Mon
# FILENAME: db_connection.py
# SPECIFICATION: This program is to connect to progresql database and create relations
# FOR: CS 4250- Assignment #2
# TIME SPENT: 1 day
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():

    # Create a database connection object using psycopg2
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            cursor_factory=RealDictCursor
        )
        return conn
    except:
        print("Database is not connected successfully")
    

def createCategory(cur, catId, catName):

    # Insert a category in the database
    sql = "Insert into category (catId, catName) Values (%s,%s)"
    recset = (catId, catName)
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    sql = "select catId from category where catName = %s"
    cur.execute(sql, (docCat,))
    cat_id = cur.fetchone()
    
    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    document = "".join(text for text in docText if text.isalnum() or text.isspace())
    num_chars = len(document)
    sql = "Insert into documents (docId, docText, docTitle, num_chars, docDate, docCat) Values (%s, %s, %s, %s, %s, %s)"
    recset = (docId, docText, docTitle, num_chars, docDate, cat_id['catid'])
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    terms = document.lower().split()
    for term in terms:
        sql = "Select term from terms where term = %s"
        cur.execute(sql, (term,))
        existing_term = cur.fetchone()

        if not existing_term:
            sql = "Insert into terms (term) Values (%s)"
            cur.execute(sql, (term,))
        sql = "Select count from indextable where term = %s and docId = %s"
        cur.execute(sql, (term, docId))
        existing_record = cur.fetchone()
     # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
        if existing_record:
            count = existing_record['count'] + 1
            sql = "Update indextable set count = %s where term = %s and docId = %s"
            cur.execute(sql, (count, term, docId))
        else:
            sql = "Insert into indextable (term, docId, count) Values (%s, %s, 1)"
            cur.execute(sql, (term, docId))
def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    sql = "Delete from indextable where docId = %s"
    cur.execute(sql, (docId,))
    sql = "Delete from terms where term not in (select term from indextable)"
    cur.execute(sql, (docId,))
    # 2 Delete the document from the database
    sql = "Delete from documents where docId = %s"
    cur.execute(sql, (docId,))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    updateDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    index = {}
    sql = "Select term, docTitle, count from indextable join documents on indextable.docId = documents.docId"
    cur.execute(sql)
    querys = cur.fetchall()

    for query in querys:
        term, doc_title, count = query['term'], query['doctitle'], query['count']
        if term in index:
            index[term][doc_title] = count
        else:
            index[term] = {doc_title: count}
    return index

def createtables(cur, conn):
    try:
        sql = "Create table category (catId integer not null, catName text not null, constraint category_pk primary key (catId))"
        cur.execute(sql)
        sql = "Create table documents (docId integer not null, docText text not null, docTitle text not null, num_chars integer not null, docDate date not null, docCat integer not null, constraint documents_pk primary key (docId), constraint documents_fk foreign key (docCat) references category (catId))"
        cur.execute(sql)
        sql = "Create table terms (term text not null, num_char integer,constraint terms_pk primary key (term))"
        cur.execute(sql)
        sql = "Create table indextable (term text not null, docId integer not null, count integer not null, constraint indextable_pk primary key (term, docId), constraint indextable_fk foreign key (term) references terms (term), constraint indextable_fk_1 foreign key (docId) references documents (docId))"
        cur.execute(sql)
        conn.commit()
    except:
        conn.rollback()
        print("There was a problem during the database creation or the database already exists")


