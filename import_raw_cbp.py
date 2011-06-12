import glob, json, os
import couchdb

def import_raw_cbp(directory, dbname="raw_cbp"):
    """Import the raw json data from the cbpscraper.
    """
    couch = couchdb.Server()
    # Create or get the database
    try:
        db = couch.create(dbname)
    except couchdb.PreconditionFailed:
        # Database already exists
        db = couch[dbname]
    for jsonfile in glob.glob(os.path.join(directory, "*.json")):
        print jsonfile
        data = json.load(open(jsonfile))
        for company in data:
            # Gebruik de url als key
            db[company["url"]] = company


if __name__ == "__main__":
    import_raw_cbp("/home/floort/devel/pim/data")


