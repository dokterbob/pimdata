import glob, json, os, sys, time, logging
import couchdb


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def import_raw_cbp(directory, dbname="raw_cbp"):
    """Import the raw json data from the cbpscraper.
    """
    couch = couchdb.Server(full_commit=False)
    # Create or get the database
    try:
        db = couch.create(dbname)
    except couchdb.PreconditionFailed:
        # Database already exists
        db = couch[dbname]


    counter = 0
    start_time = time.time()
    for jsonfile in glob.glob(os.path.join(directory, "*.json")):
        logging.debug('Reading %s', jsonfile)
        data = json.load(open(jsonfile))
        for company in data:
            assert company['url'].startswith('http://www.cbpweb.nl/asp/ORMelding.asp?id=')
            doc_id = company['url'][42:]

            try:
                db[doc_id] = company
            except couchdb.http.ResourceConflict:
                # Update the record
                logger.info('Found existing object for %s, updating.',
                            doc_id)

                doc = db[doc_id]
                doc.update(company)
                db[doc_id] = doc

        counter += 1

        if counter % 100 == 0:
            db.commit()
            t = time.time()-start_time
            logger.info('Wrote %d records to database in %f seconds (%f/s)',
                        counter, t, counter/t)

    logger.info('Wrote %d records to database in %f seconds',
                counter, time.time()-start_time)
    
    # Create views
    db["_design/tagging"] = {
        "language": "javascript",
        "views": {
            "wordfeq": {
                "map": """
function(doc) {
    var wordlist = {};
    for (var melding in doc.meldingen) {
        var splitlist = doc.meldingen[melding]["description"].split(" ")
        for (var i in splitlist) {
            if (splitlist[i] in wordlist) {
                wordlist[splitlist[i]] += 1;
            } else {
                wordlist[splitlist[i]] = 1;
            }
        }
    }
    emit(doc.id, wordlist);
}"""
            }
        }
    }


if __name__ == "__main__":
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print 'You monkey!'
        exit(-1)

    import_raw_cbp(path)


