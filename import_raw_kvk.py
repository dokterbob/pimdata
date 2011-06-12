import sys, couchdb, logging, time
from utils import get_csv_reader


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def import_raw_kvk(csv_file, dbname="raw_kvk"):
    """Import the raw json data from Open KVK.
    """
    couch = couchdb.Server()

    if dbname in couch: # Start fresh
        couch.delete(dbname)
    db = couch.create(dbname)

    reader = get_csv_reader(csv_file, 'utf-8')

    counter = 0
    start_time = time.time()
    for row in reader:
        records = []

        # Fieldnames from http://api.openkvk.nl/
        fields = {
            'kvk': row[0],
            'bedrijfsnaam': row[1],
            'kvks': row[2],
            'sub': row[3],
            'adres': row[4],
            'postcode': row[5],
            'plaats': row[6],
            'type': row[7],
            'website': row[8]
        }

        records.append(fields)
        counter += 1

        if counter % 100000 == 0:
            db.update(records)

            t = time.time()-start_time
            logger.info('Wrote %d records to database in %f seconds (%f/s)',
                        counter, t, counter/t)

    logger.info('Wrote %d records to database in %f seconds',
                counter, time.time()-start_time)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        csv_file = sys.argv[1]
    else:
        print 'You monkey!'
        exit(-1)

    import_raw_kvk(csv_file)


