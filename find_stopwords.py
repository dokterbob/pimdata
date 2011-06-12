import couchdb


def find_stopwords_cbp_name(db_name="raw_cbp"):
    couch = couchdb.Server()
    db = couch[db_name]
    
    result = db.view('names/word_count', group=True)
    words = [(row.key, row.value) for row in result.rows if row.value > 2]
    
    words = sorted(words, key=lambda word: word[1], reverse=True)
  
    for word in words[:200]:
        print word[0], word[1]


if __name__ == "__main__":
    find_stopwords_cbp_name()

