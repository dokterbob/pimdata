import couchdb

couch = couchdb.Server()

# Create or get the database
try:
    db = couch.create('mydb')
except couchdb.PreconditionFailed:
    # Database already exists
    db = couch['mydb']
    

