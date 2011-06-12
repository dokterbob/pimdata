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

# Query from the server
# get_john = \
# '''
# function (doc) {
#     if (doc.type == 'person' && doc.name.search('Doe') ) {
#         emit(doc.name, doc);
#     }
# }
# '''
get_john = \
'''
function (doc) {
    if (doc.type == 'person' && doc.name.search('Doe')) {
        emit(doc.name, doc);
    }
}
'''
res = db.query(get_john)

from IPython.Shell import IPShellEmbed
ipshell = IPShellEmbed()

ipshell() # this call anywhere in your program will start IPython
