import glob, json, os, sys, time, logging
import couchdb


logging.basicConfig(level=logging.DEBUG)
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
            doc_id = company['url']
            doc = db.get(doc_id)

            if doc:
                logger.info('Found existing object for %s, updating.',
                            doc_id)
                doc.update(company)
            else:
                doc = company

            db[doc_id] = doc

        counter += 1

        if counter % 100 == 0:
            logger.info('Wrote %d records to database in %f seconds',
                        counter, time.time()-start_time)

            if counter % 1000 == 0:
                logger.info('Committing changes.')
                db.commit()

    logger.info('Wrote %d records to database in %f seconds',
                counter, time.time()-start_time)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        path = sys.argv[1]
    else:
        print 'You monkey!'
        exit(-1)

    import_raw_cbp(path)


