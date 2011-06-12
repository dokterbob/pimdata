import couchdb

couch = couchdb.Server()

# Create or get the database
try:
    db = couch.create('mydb')
except couchdb.PreconditionFailed:
    # Database already exists
    db = couch['mydb']


from uuid import uuid4

# UUID Generates unique document ID's
doc = {'_id': uuid4().hex, 'type': 'person', 'name': 'John Doe'}
db.save(doc)

# from couchdb.client import Document
# doc = Document()