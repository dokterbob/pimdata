import logging
from difflib import get_close_matches

import couchdb

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

import Levenshtein
def get_matches(needle, haystack, ratio=0.6):
    needle = unicode(needle)

    result = {}
    for s in haystack:
        if s != needle:
            assert unicode(s)

            distance = Levenshtein.jaro(needle, unicode(s))
            if distance > ratio:
                result[s] = distance

    return result


def similar_company_cbp(db_name_in="raw_cbp", db_name_out="raw_cbp_similarities"):
    """ Find similar company names in CBP database """
    couch = couchdb.Server()

    db = couch[db_name_in]

    if db_name_out in couch: # Start fresh
        couch.delete(db_name_out)
    db_out = couch.create(db_name_out)

    # company_names = db.view('names/company_names')
    # logger.info('Fetched company names, returned %d rows', company_names.total_rows)

    names = db.view('names/names')
    logger.info('Fetched by name, returned %d rows', names.total_rows)

    name_list = set()
    counter = 0
    for row in names.rows:
        name = row.key
        name_list.add(name)

        counter += 1

        if counter % 100 == 0:
            logger.debug('Added %d company names to list.', counter)

    counter = 0
    for row in names:
        name = row.key
        company = row.value

        matches = get_matches(name, name_list, ratio=0.8)
        if matches:
            # Write the results back to the database
            logger.info('Found matches for %s: %s', name, matches)

            # Replace names by their equivalent company names
            new_matches = {}
            for match_name in matches.keys():
                # There *might* be multiple matches per name
                for row in names[match_name].rows:
                    new_matches[row.value['_id']] = matches[match_name]

                assert new_matches

            doc_id = company['_id']
            try:
                db_out[doc_id] = matches
            except couchdb.http.ResourceConflict:
                # Update the record
                logger.info('Found existing object for %s, updating.',
                            doc_id)

                doc = db_out[doc_id]
                doc.update(matches)
                db_out[doc_id] = doc

        counter += 1

        if counter % 100 == 0:
            logger.debug('Looked for matches in %s company names', counter)


if __name__ == "__main__":
    similar_company_cbp()

