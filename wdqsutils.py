import requests
import datetime
from lxml import etree
from collections import defaultdict


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


def get_taxa_missing_irmng(highertaxon_qid, highertaxon_rank, rank="genus"):
    url="https://query.wikidata.org/sparql"
    ranks = {
        'genus' : 'Q34740',
        'family' : 'Q35409',
    }
    rank_qid = ranks[rank]
    query = """PREFIX gas: <http://www.bigdata.com/rdf/gas#>
    SELECT DISTINCT ?item ?itemLabel ?taxonName
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
      ?item wdt:P105 wd:%s; 
      FILTER ( NOT EXISTS { ?item wdt:P5055 ?identifier. } )
      ?item wdt:P225 ?taxonName
      OPTIONAL { ?item wdt:P171 ?linkTo }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "en" .
        ?item rdfs:label ?itemLabel .
      }
    }"""% (highertaxon_qid, rank_qid)
    r1 = requests.get(url, params={'query': query})
    out = defaultdict(list)
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
            try:
                res_dict = {ee.get('name') : ee for ee in e.findall('binding')}
                qid = res_dict['item'].find('uri').text.split("/")[-1]
                taxonName = res_dict['taxonName'].find('literal').text
                out[taxonName].append({ 'qid' : qid, 'taxonName' : taxonName })
            except:
                print("Error in parsing the following:")
                print(res_dict)
                qid = res_dict['item'].find('uri').text.split("/")[-1]
                print(qid)
    return r1, out

def quickstatements_taxon_add_IRMNG_ID(highertaxon_qid, highertaxon_name, highertaxon_rank, rank="genus"):
    """Match taxa without IRMNG IDs to IRMNG records

    Match by taxon name and higher taxon to the IRMNG database, and only report
    a result if there is only one hit, to avoid homonyms or other problematic
    records. There is still a risk of homonyms slipping through, because we do
    not check author, year, or publication (those need to be reconciled first).

    Writes Quickstatements v2 directly to a CSV file.

    Parameters
    ----------
    highertaxon_qid : str
        QID of the higher taxon of interest
    highertaxon_name : str
        Taxon name of the higher taxon, used to match records retrieved from IRMNG.
    highertaxon_rank : str
        Rank of higher taxon, all lowercase, used to match records retrieved from IRMNG.
    rank : str
        Rank of taxon items in Wikidata to check, one of 'genus', 'family'
        (IRMNG does not record subgeneric ranks).
    """
    r1, out = get_taxa_missing_irmng(highertaxon_qid, highertaxon_rank, rank=rank)
    if r1.ok:
        print(f"{str(len(out))} Wikidata items found without IRMNG IDs")
        with open(f"add_P5055_{highertaxon_name}_{highertaxon_rank}.{rank}.csv", "w") as fh:
            fh.write("qid,P5055,S248,s813,#\n")
            for taxonName in out:
                if len(out[taxonName]) == 1: 
                    irmng_url = "https://irmng.org/rest/AphiaRecordsByName/" + taxonName
                    params = { 'like' : 'false', 'marine_only' : 'false' }
                    r = requests.get(irmng_url, params=params)
                    if r.ok and r.status_code == 200:
                        ret = [
                            rec['IRMNG_ID'] 
                            for rec in r.json() 
                            if highertaxon_rank in rec 
                            and rec[highertaxon_rank] == highertaxon_name
                        ]
                        if len(ret) == 1:
                            out[taxonName][0]['irmng_id'] = ret[0]
                            out[taxonName][0]['retrieved'] = datetime.datetime.utcnow(
                                ).strftime(
                                    "+%Y-%m-%dT00:00:00Z/11"
                                ) # for quickstatements
                            fh.write(','.join(
                                [
                                    out[taxonName][0]['qid'],
                                    '"""' + str(out[taxonName][0]['irmng_id']) + '"""',
                                    'Q51885189',
                                    out[taxonName][0]['retrieved'],
                                    f'matched by name and {highertaxon_rank} {highertaxon_name} to IRMNG',
                                ]
                            ))
                            fh.write("\n")
                
