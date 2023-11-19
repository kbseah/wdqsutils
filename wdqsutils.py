import requests
from lxml import etree


def get_taxa_missing_descs(higher_taxon_qid, langcode, rank='species'):
    """Query Wikidata for taxon items with missing descriptions in language

    Parameters
    ----------
    higher_taxon_qid : str
        QID of higher taxon containing taxa of interest
    langcode : str
        Language code for description (e.g. "en", "zh")
    rank : str
        Rank of taxa to retrieve. One of: "species", "genus", "family"

    Returns
    -------
    (r1, out)
    r1 : requests.Request
        Raw Request object returned from server
    out : list
        List of QIDs for the taxon items matching the query
    """
    url="https://query.wikidata.org/sparql"
    ranks = {
        'species' : 'Q7432',
        'genus' : 'Q34740',
        'family' : 'Q35409',
    }
    rank_qid = ranks[rank]
    query="""PREFIX gas: <http://www.bigdata.com/rdf/gas#>
    SELECT DISTINCT ?item ?itemLabel ?itemDesc
    WHERE
    {
      SERVICE gas:service {
        gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.SSSP" ;
                    gas:in wd:%s;
                    gas:traversalDirection "Reverse" ;
                    gas:out ?item ;
                    gas:out1 ?depth ;
                    gas:maxIterations 10 ;
                    gas:linkType wdt:P171 .
      }
      ?item wdt:P31 wd:Q16521;
            wdt:P105 wd:%s;
      OPTIONAL { ?item wdt:P171 ?linkTo }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "%s" .
        ?item rdfs:label ?itemLabel .
      }
      FILTER(
        NOT EXISTS {
          ?item schema:description ?lang_desc.
          FILTER(LANG(?lang_desc) = "%s")
        }
      )
    }
    """ % (higher_taxon_qid, rank_qid, langcode, langcode)
    r1 = requests.get(url, params={'query': query})
    out = []
    if r1.ok:
        # Parse XML
        rtree = etree.fromstring(
            r1.text.encode()
        ) # .encode otherwise ValueError "Unicode strings with encoding declaration are not supported"
        # Strip namespace prefix
        for e in rtree.getiterator():
            e.tag = etree.QName(e).localname
    
        # Translate results to dictionary
        for e in rtree.iterdescendants('result'):
            res_dict = {ee.get('name') : ee for ee in e.findall('binding')}
            qid = res_dict['item'].find('uri').text.split("/")[-1]
            out.append(qid)
    return r1, out


def quickstatements_taxon_add_desc(higher_taxon_qid, rank, desc, langcode):
    """Generate Quickstatements to add descriptions to taxon items

    Search for taxon items (descendants of a specified higher taxon) without
    descriptions in a target language, and generate QuickStatements v2 (CSV
    format) batch commands to add the same provided description to each of them.

    For example, find all ciliate species without French descriptions and add
    "espèce de ciliés" to each of them.

    Output written to file.

    Parameters
    ----------
    higher_taxon_qid : str
        QID of the taxon of interest
    rank : str
        One of "species", "genus", "family"
    desc : str
        Description to add to the taxon items
    langcode : str
        Language code
    """
    r, out = get_taxa_missing_descs(
        higher_taxon_qid,langcode,rank
    )
    if r.ok:
        print(f"Number of records: {len(out)}")
        filename = f"add_D{langcode}_{higher_taxon_qid}_{rank}.csv"
        with open(filename, "w") as fh:
            quickstatements_header = ['qid','D'+langcode,'#']
            fh.write(','.join(quickstatements_header))
            fh.write("\n")
            for qid in out:
                line = [qid, desc, f'add {langcode} descriptions']
                fh.write(','.join(line))
                fh.write("\n")
        print(f"Quickstatements written to file: {filename}")
    else:
        print(f"Request to Wikidata server failed with status code: {r.status_code}")


def get_articles_missing_descs(periodical_qid, langcode):
    """Query Wikidata for scholarly articles with missing descriptions in language

    Query by periodical to avoid timeout, because there are too many items that
    are instances of "scholarly article".

    Parameters
    ----------
    periodical_qid : str
        QID of periodical in which the articles were published
    langcode : str
        Language code for description (e.g. "en", "zh")

    Returns
    -------
    (r1, out)
    r1 : requests.Request
        Raw Request object returned from server
    out : list
        List of tuples of (QID, year) for the articles matching the query
    """
    url="https://query.wikidata.org/sparql"
    query="""SELECT DISTINCT ?item ?date
    WHERE
    {
      ?item wdt:P1433 wd:%s;
            wdt:P31 wd:Q13442814;
            wdt:P577 ?date.
      FILTER(
        NOT EXISTS {
          ?item schema:description ?lang_desc.
          FILTER(LANG(?lang_desc) = "%s")
        }
      )
    } 
    """ % (periodical_qid, langcode)
    r1 = requests.get(url, params={'query': query})
    out = []
    if r1.ok:
        # Parse XML
        rtree = etree.fromstring(
            r1.text.encode()
        ) # .encode otherwise ValueError "Unicode strings with encoding declaration are not supported"
        # Strip namespace prefix
        for e in rtree.getiterator():
            e.tag = etree.QName(e).localname
    
        # Translate results to dictionary
        for e in rtree.iterdescendants('result'):
            res_dict = {ee.get('name') : ee for ee in e.findall('binding')}
            qid = res_dict['item'].find('uri').text.split("/")[-1]
            year = res_dict['date'].find('literal').text.split("-")[0]
            out.append((qid,year))

    return r1, out


def quickstatements_articles_add_desc(periodical_qid, langcode, desc_prefix="scholarly article published in ", desc_suffix=""):
    r, out = get_articles_missing_descs(
        periodical_qid, langcode
    )
    if r.ok:
        print(f"Number of records: {len(out)}")
        filename = f"add_D{langcode}_{periodical_qid}_articles.csv"
        with open(filename, "w") as fh:
            quickstatements_header = ['qid','D'+langcode,'#']
            fh.write(','.join(quickstatements_header))
            fh.write("\n")
            for qid, year in out:
                line = [
                    qid,
                    desc_prefix + year + desc_suffix,
                    f'add {langcode} descriptions'
                ]
                fh.write(','.join(line))
                fh.write("\n")
        print(f"Quickstatements written to file: {filename}")
    else:
        print(f"Request to Wikidata server failed with status code: {r.status_code}")


