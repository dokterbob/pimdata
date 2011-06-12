import logging
from difflib import get_close_matches

import couchdb

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def similar_company_cbp(dbname="raw_cbp"):
    """ Find similar company names in CBP database """
    couch = couchdb.Server()
    
    db = couch[dbname]

    # company_names = db.view('names/company_names')
    # import ipdb; ipdb.set_trace()
    # logger.info('Fetched company names, returned %d rows', company_names.total_rows)

    names = db.view('names/names')
    logger.info('Fetched by name, returned %d rows', names.total_rows)

    name_list = []
    counter = 0
    for row in names.rows:
        name = row.key
        name_list.append(name)
        
        counter += 1
        
        if counter % 100 == 0:
            logger.debug('Added %d company names to list.', counter)
    
    counter = 0
    for row in names:
        name = row.key
        company = row.value

        matches = get_close_matches(name, name_list, 5)
        if matches:
            logger.info('Found matches for %s: %s', name, matches)
        
        counter += 1
        
        if counter % 100 == 0:
            logger.debug('Looked for matches in %s company names', counter)
            


if __name__ == "__main__":    
    similar_company_cbp()
    
    