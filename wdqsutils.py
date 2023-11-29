import requests
import datetime
import time
import re
from lxml import etree
from collections import defaultdict


def parse_sparql_return(r, uris, literals):
    """
    Parse XML returned by Wikidata SPARQL query and return a list of dicts
    
    Parameters
    ----------
    r : requests.models.Response
        Object returned by requests.request
    uris : list
        Names of bindings that are to be interpreted as Wikidata URIs
    literals : list
        Names of bindings that are to be interpreted as text strings
    """
    out = []
    if r.ok:
        # Parse XML
        rtree = etree.fromstring(
            r.text.encode()
        ) # .encode otherwise ValueError "Unicode strings with encoding declaration are not supported"
        # Strip namespace prefix
        for e in rtree.getiterator():
            e.tag = etree.QName(e).localname

        # Translate results to dictionary
        for e in rtree.iterdescendants('result'):
            res_dict = {ee.get('name') : ee for ee in e.findall('binding')}
            for key in uris:
                if key in res_dict:
                    res_dict[key] = res_dict[key].find('uri').text.split("/")[-1]
            for key in literals:
                if key in res_dict:
                    res_dict[key] = res_dict[key].find('literal').text
            out.append(res_dict)
        return(out)
    else:
        print("Response not ok")


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
    SELECT DISTINCT ?item ?itemLabel ?parentTaxonRankLabel ?parentTaxonName
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
      ?item wdt:P31 wd:Q16521; # avoid fossil taxa
            wdt:P105 wd:%s;
            wdt:P171 ?parentTaxon.
      ?parentTaxon wdt:P225 ?parentTaxonName;
                   wdt:P105 ?parentTaxonRank.
      OPTIONAL { ?item wdt:P171 ?linkTo }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "%s" .
        ?item rdfs:label ?itemLabel .
        ?parentTaxonRank rdfs:label ?parentTaxonRankLabel .
      }
      FILTER(
        NOT EXISTS {
          ?item schema:description ?lang_desc.
          FILTER(LANG(?lang_desc) = "%s")
        }
      )
    }
    """ % (higher_taxon_qid, rank_qid, langcode, langcode)
    r = requests.get(url, params={'query': query})
    out = parse_sparql_return(r, ['item'], ['itemLabel','parentTaxonRankLabel','parentTaxonName'])
    return r, out


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
            for rec in out:
                line = [rec['item'], desc, f'add {langcode} descriptions']
                fh.write(','.join(line))
                fh.write("\n")
        print(f"Quickstatements written to file: {filename}")
    else:
        print(f"Request to Wikidata server failed with status code: {r.status_code}")


def quickstatements_taxon_add_desc_long(higher_taxon_qid, rank, vernacular_name, langcode):
    """Generate Quickstatements to add descriptions to taxon items

    Search for taxon items (descendants of a specified higher taxon) without
    descriptions in a target language, and generate QuickStatements v2 (CSV
    format) batch commands to add the same provided description to each of them.

    For example, find all ciliate species without English descriptions and add
    "species of ciliates in the genus XXX" to each of them.

    Output written to file.

    Parameters
    ----------
    higher_taxon_qid : str
        QID of the taxon of interest
    rank : str
        One of "species", "genus", "family"
    vernacular_name : str
        Vernacular name (plural form) to use in the description
    langcode : str
        Language code: en or de only
    """
    r, out = get_taxa_missing_descs(
        higher_taxon_qid,langcode,rank
    )
    rankLang = {
        'de' : {'species':'Art', 'genus': 'Gattung', 'family':'Familie'},
    }
    if r.ok:
        print(f"Number of records: {len(out)}")
        filename = f"add_D{langcode}_{higher_taxon_qid}_{rank}.csv"
        with open(filename, "w") as fh:
            quickstatements_header = ['qid','D'+langcode,'#']
            fh.write(','.join(quickstatements_header))
            fh.write("\n")
            for rec in out:
                if 'parentTaxonName' in rec and 'parentTaxonRankLabel' in rec:
                    if langcode == 'en':
                        desc = f"{rank} of {vernacular_name} in the {rec['parentTaxonRankLabel']} {rec['parentTaxonName']}"
                    if langcode == 'de':
                        desc = f"{rankLang['de'][rank]} der {vernacular_name} der {rec['parentTaxonRankLabel']} {rec['parentTaxonName']}"
                    line = [rec['item'], desc, f'add {langcode} descriptions']
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
    r = requests.get(url, params={'query': query})
    out = parse_sparql_return(r, ['item'], ['date'])
    for rec in out:
        rec['year'] = rec['date'].split("-")[0]
    return r, out



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
    r = requests.get(url, params={'query': query})
    out = parse_sparql_return(r, ['item'], ['date'])
    for rec in out:
        rec['year'] = rec['date'].split("-")[0]
    return r, out


def quickstatements_articles_add_desc(periodical_qid, langcode):
    """Quickstatements to add missing descriptions for scholarly articles

    Parameters
    ----------
    periodical_qid : str
        Wikidata QID of the periodical the articles are published in
    langcode : str
        Language code for language of description. One of: en, de, zh, zh-s, zh-hant
    """
    r, out = get_articles_missing_descs(
        periodical_qid, langcode
    )
    desc_prefix = {
        'en' : 'scholarly article published in ',
        'de' : 'im Jahr ',
        'ms' : 'makalah ilmiah yang diterbitkan pada ',
        'zh' : '',
        'zh-hans' : '',
        'zh-hant' : '',
    }
    desc_suffix = {
        'en' : '',
        'de' : ' veröffentlichter wissenschaftlicher Artikel',
        'ms' : '',
        'zh' : '年學術文章',
        'zh-hans' : '年学术文章',
        'zh-hant' : '年學術文章',
    }
    if r.ok:
        print(f"Number of records: {len(out)}")
        filename = f"add_D{langcode}_{periodical_qid}_articles.csv"
        with open(filename, "w") as fh:
            quickstatements_header = ['qid','D'+langcode,'#']
            fh.write(','.join(quickstatements_header))
            fh.write("\n")
            for rec in out:
                if 'year' in rec:
                    line = [
                        rec['item'],
                        desc_prefix[langcode] + rec['year'] + desc_suffix[langcode],
                        f'add {langcode} descriptions'
                    ]
                    fh.write(','.join(line))
                    fh.write("\n")
        print(f"Quickstatements written to file: {filename}")
    else:
        print(f"Request to Wikidata server failed with status code: {r.status_code}")


def get_taxa_missing_identifier(highertaxon_qid, db='irmng', rank="genus"):
    url="https://query.wikidata.org/sparql"
    ranks = {
        'species' : 'Q7432',
        'genus' : 'Q34740',
        'family' : 'Q35409',
    }
    dbqids = {
        'irmng' : 'P5055',
        'gbif' : 'P846',
        'index_fungorum' : 'P1391',
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
      FILTER ( NOT EXISTS { ?item wdt:%s ?identifier. } )
      ?item wdt:P225 ?taxonName
      OPTIONAL { ?item wdt:P171 ?linkTo }
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "en" .
        ?item rdfs:label ?itemLabel .
      }
    }"""% (highertaxon_qid, rank_qid, dbqids[db])
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
                print(etree.tostring(e))
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
        Rank of taxon items in Wikidata to check, one of 'species', 'genus', 'family'
    """
    r1, out = get_taxa_missing_identifier(highertaxon_qid, 'irmng', rank=rank)
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


def quickstatements_taxon_add_GBIF_ID(highertaxon_qid, highertaxon_name, highertaxon_rank, rank="genus"):
    """Match taxa without GBIF IDs to GBIF records

    Parameters
    ----------
    highertaxon_qid : str
        QID of the higher taxon of interest
    highertaxon_name : str
        Taxon name of the higher taxon, used to match records retrieved from GBIF.
    highertaxon_rank : str
        Rank of higher taxon, all lowercase, used to match records retrieved from GBIF.
    rank : str
        Rank of taxon items in Wikidata to check, one of 'species', 'genus', 'family'
    """
    r1, out = get_taxa_missing_identifier(highertaxon_qid, 'gbif', rank=rank)
    gbif_url = "https://api.gbif.org/v1/species/match"
    if r1.ok:
        print(f"{str(len(out))} Wikidata items found without GBIF IDs")
        with open(f"add_P846_{highertaxon_name}_{highertaxon_rank}.{rank}.csv", "w") as fh:
            fh.write("qid,P846,S248,s813,#\n")
            for name in out:
                if len(out[name]) == 1:
                    params = {
                        'strict' : 'true',
                        highertaxon_rank : highertaxon_name,
                        'rank' : rank,
                        'name' : out[name][0]['taxonName']
                    }
                    r = requests.get(gbif_url, params=params)
                    # Multiple hits will result in matchType NONE
                    if (
                        r.ok and r.status_code == 200
                        and r.json()['matchType'] == 'EXACT'
                        and highertaxon_rank in r.json()
                        and r.json()[highertaxon_rank] == highertaxon_name
                    ):
                        # GBIF API still returns a match even if higher taxon
                        # does not match, so we have to filter it ourselves (?)
                        out[name][0]['gbif_id'] = r.json()['usageKey']
                        out[name][0]['retrieved'] = datetime.datetime.utcnow(
                            ).strftime(
                                "+%Y-%m-%dT00:00:00Z/11"
                            ) # for quickstatements
                        fh.write(','.join(
                            [
                                out[name][0]['qid'],
                                '"""' + str(out[name][0]['gbif_id']) + '"""',
                                'Q1531570',
                                out[name][0]['retrieved'],
                                f'matched by name and {highertaxon_rank} {highertaxon_name} to GBIF',
                            ]
                        ))
                        fh.write("\n")


def quickstatements_taxon_add_IndexFungorum_ID(highertaxon_qid, highertaxon_name, highertaxon_rank, rank="genus"):
    """Match fungal taxa without Index Fungorum IDs to Index Fungorum records

    Parameters
    ----------
    highertaxon_qid : str
        QID of the higher taxon of interest
    rank : str
        Rank of taxon items in Wikidata to check, one of 'species', 'genus', 'family'
    """
    r1, out = get_taxa_missing_identifier(highertaxon_qid, 'index_fungorum', rank=rank)
    rank_abbrevs = {
        'species' : 'sp.',
        'genus' : 'gen.',
        'family' : 'fam.',
    }
    url = "http://www.indexfungorum.org/ixfwebservice/fungus.asmx/NameSearchDs"
    if r1.ok:
        print(f"{str(len(out))} Wikidata items found without Index Fungorum IDs")
        with open(f"add_P1391_{highertaxon_name}_{highertaxon_rank}.{rank}.csv", "w") as fh:
            fh.write("qid,P1391,S248,s813,#,P6507,S248,s813\n")
            for name in out:
                if len(out[name]) == 1:
                    params = {
                        'SearchText' : out[name][0]['taxonName'],
                        'AnywhereInText' : 'false',
                        'MaxNumber' : '5',
                    }
                    r = requests.get(url, params=params)
                    # Multiple hits will result in matchType NONE
                    if r.ok and r.status_code == 200:
                        rtree = etree.fromstring(r.text.encode())
                        for e in rtree.getiterator():
                            e.tag = etree.QName(e).localname
                        recs = []
                        for e in rtree.iterdescendants('IndexFungorum'):
                            rec = {
                                g.tag : g.text for g in e.iterchildren()
                            }
                            if (
                                'AUTHORS' in rec
                                and rec['NAME_x0020_OF_x0020_FUNGUS'] == out[name][0]['taxonName']
                                and rec['INFRASPECIFIC_x0020_RANK'] == rank_abbrevs[rank]
                            ):
                                recs.append(rec)
                        if len(recs) == 1:
                            out[name][0]['id'] = recs[0]['RECORD_x0020_NUMBER']
                            out[name][0]['taxon_author_citation'] = recs[0]['AUTHORS']
                            out[name][0]['retrieved'] = datetime.datetime.utcnow(
                                ).strftime(
                                    "+%Y-%m-%dT00:00:00Z/11"
                                ) # for quickstatements
                            fh.write(','.join(
                                [
                                    out[name][0]['qid'],
                                    '"""' + str(out[name][0]['id']) + '"""',
                                    'Q1860469',
                                    out[name][0]['retrieved'],
                                    f'matched by name and rank to Index Fungorum',
                                    '"""' + str(out[name][0]['taxon_author_citation']) + '"""',
                                    'Q1860469',
                                    out[name][0]['retrieved'],
                                ]
                            ))
                            fh.write("\n")


def get_fungi_missing_taxon_author_citation(highertaxon_qid):
    url="https://query.wikidata.org/sparql"
    query="""PREFIX gas: <http://www.bigdata.com/rdf/gas#>

    SELECT DISTINCT ?item ?taxonName ?yearTaxonPublication ?indexFungorum
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
      OPTIONAL { ?item wdt:P171 ?linkTo }
      ?item wdt:P1391 ?indexFungorum;
            wdt:P225 ?taxonName.
      OPTIONAL { 
        ?item p:P225 [
          pq:P574 ?yearTaxonPublication
        ]
      }
      FILTER(
        NOT EXISTS {
          ?item wdt:P6507 ?taxonAuthorCitation .
        }
      )
    }""" % (highertaxon_qid)
    r = requests.get(url, params={'query': query})
    out = parse_sparql_return(r, ['item'], ['indexFungorum','taxonName','yearTaxonPublication'])
    return (r, out)


def quickstatements_taxon_author_citations_from_index_fungorum(highertaxon_qid):
    r, o = get_fungi_missing_taxon_author_citation(highertaxon_qid)
    if not r.ok:
        print(f"Request not ok, status code {r.status_code}")
        return r
    print(f"{str(len(o))} items without taxon author citations")
    url = "http://www.indexfungorum.org/ixfwebservice/fungus.asmx/NameByKey"
    authors_statements = [["qid","P6507","S248","s813"],] # initialize with header
    years_statements = [["qid","P225","qal574","S248","s813"],] # initialize with header
    for rec in o:
        params = { 'NameKey' : rec['indexFungorum'] }
        rr = requests.get(url,params=params)
        if rr.ok and rr.status_code == 200:
            rtree = etree.fromstring(rr.text.encode())
            for e in rtree.getiterator():
                e.tag = etree.QName(e).localname
            for e in rtree.iterdescendants('IndexFungorum'):
                rrec = { g.tag : g.text for g in e.iterchildren() }
                if 'AUTHORS' in rrec and 'YEAR_x0020_OF_x0020_PUBLICATION' in rrec:
                    if 'yearTaxonPublication' not in rec:
                        out_y = [
                            rec['item'],
                            "\"\"\"" + rec['taxonName'] + "\"\"\"",
                            "+" + rrec['YEAR_x0020_OF_x0020_PUBLICATION'] + "-01-01T00:00:00Z/9", # /9 - year precision
                            "Q1860469",
                            datetime.datetime.utcnow().strftime("+%Y-%m-%dT00:00:00Z/11"), # /11 - day
                        ]
                        years_statements.append(out_y)
                    out_a = [
                        rec['item'],
                        "\"\"\"" + rrec['AUTHORS'] + "\"\"\"",
                        "Q1860469",
                        datetime.datetime.utcnow().strftime("+%Y-%m-%dT00:00:00Z/11"),
                    ]
                    authors_statements.append(out_a)
    # Write quickstatements to files
    with open(f"add_taxon_author_citation_{highertaxon_qid}.csv","w") as fh:
        for r in authors_statements:
            fh.write(",".join(r))
            fh.write("\n")
    with open(f"add_year_taxon_pub_{highertaxon_qid}.csv","w") as fh:
        for r in years_statements:
            fh.write(",".join(r))
            fh.write("\n")


def get_taxon_author_citations_but_no_taxon_author(highertaxon_qid):
    """Get Wikidata items for taxa with taxon author citation but not taxon authors

    Aim is to use parse taxon author citation to find Wikidata items 
    corresponding to the taxon authors and link them with new taxon author
    statements (as qualifiers to the taxon name statement).

    Parameters
    ----------
    highertaxon_qid : str
        QID of higher taxon containing items of interest.
        Example: Q831743 for Russulaceae (fungi)
    """
    query="""SELECT ?item ?taxonName ?taxonAuthorCitation WHERE {
      SERVICE gas:service {
        gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.SSSP";
          gas:in wd:%s;
          gas:traversalDirection "Reverse";
          gas:out ?item;
          gas:out1 ?depth;
          gas:maxIterations 10 ;
          gas:linkType wdt:P171.
      }
      ?item wdt:P6507 ?taxonAuthorCitation;
            wdt:P225 ?taxonName.
      FILTER(NOT EXISTS {
        ?item p:P225 _:b3.
        _:b3 pq:P405 ?taxonAuthor.
      })
    }""" % (highertaxon_qid)
    r = requests.get("https://query.wikidata.org/sparql", params={'query' : query})
    o = parse_sparql_return(r, ['item'], ['taxonName', 'taxonAuthorCitation'])
    if r.ok:
        return r, o


def parse_botanical_taxon_author_citation(citation):
    """Parse botanical taxon author citation to component authors

    Discard basionym authors (within parentheses), separate ex taxon authors (P697).
    Remove spaces after periods to follow format in IPNI database

    Parameters
    ----------
    citation : str
        Taxon author citation, e.g. "(B.C. Zhang & Y.N. Yu) Trappe, T. Lebel & Castellano"

    Returns
    -------
    dict of lists, keyed by 'auth' (taxon authors) and 'ex_auth' (ex taxon authors).
    """
    if citation.count(')') == 1:
        # Remove basionym authors
        citation = citation.split(')')[1]
    elif citation.count(')') > 1:
        print(f"Too many parens in this citation: {citation}")
        return
    if ':' in citation or ';' in citation or '?' in citation:
        print(f"Unrecognized punctuation in this citation: {citation}")
        return
    if citation.count(' in ') == 1:
        citation = citation.split(' in ')[0]
    if citation.count(' ex ') > 1:
        print(f"Too many 'ex' in this citation: {citation}")
        return
    elif citation.count(' ex ') == 1:
        [ex_auth, auth] = citation.split(' ex ')
        ex_auth = re.split(r',|&', ex_auth)
        ex_auth = [a.replace('. ','.').rstrip().lstrip() for a in ex_auth]
        auth = re.split(r',|&', auth)
        auth = [a.replace('. ','.').rstrip().lstrip() for a in auth]
        return { 'ex_auth' : ex_auth, 'auth' : auth }
    elif citation.count(' ex ') == 0:
        auth = re.split(r',|&', citation)
        auth = [a.replace('. ','.').rstrip().lstrip() for a in auth]
        return { 'auth' : auth }


def get_items_from_botanical_author_citation(auths):
    """Look up Wikidata QIDs for botanical authors by author name abbreviation

    Query is broken into 25-value chunks with 1 second pauses in between.

    Parameters
    ----------
    auths : list
        List of author name abbreviations, no spaces after periods.

    Returns
    -------
    dict
        Dict keyed by author name abbreviations with Wikidata QIDs as values.
        QIDs not found are silently ignored.
    """
    out = []
    chunksize = 25 # avoid error 414
    for chunk in [auths[i:i+chunksize] for i in range(0, len(auths), chunksize)]:
        vals = " ".join([f'"{a}"' for a in chunk])
        query = """SELECT ?item ?value ?itemLabel WHERE {
          VALUES ?value {
            %s
          }
          ?item wdt:P428 ?value.
          SERVICE wikibase:label {
            bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en".
            ?item rdfs:label ?itemLabel.
          }
        }""" % (vals)
        r = requests.get("https://query.wikidata.org/sparql", params={'query' : query})
        o = parse_sparql_return(r, ['item'], ['value', 'itemLabel'])
        if r.ok:
            out.extend(o)
        else:
            print(chunk)
        time.sleep(1) # avoid error 429
    auth2qid = {r['value'] : r['item'] for r in out}
    return(auth2qid)


def quickstatements_taxon_authors_from_citations(highertaxon_qid):
    """Generate QuickStatements to add taxon author from taxon author citation strings

    Parse taxon author citation strings to get author abbreviations (works for
    botanical/fungal authors only, because these have a standardized form,
    whereas zoological author abbreviations are not standardized and may not be
    unique to a given author.)

    Look up taxon author items and link these to the corresponding taxon items
    as qualifiers to the taxon name statement.

    Generates two QuickStatements files, one for taxon author statements, the
    other for ex taxon author statements.

    Parameters
    ----------
    highertaxon_qid : str
        Wikidata QID of the higher taxon of interest.
    """
    r, o = get_taxon_author_citations_but_no_taxon_author(highertaxon_qid)
    print(f"{str(len(o))} items found without taxon author qualifiers but with taxon author citations")
    auth_parsed = {}
    for rec in o:
        parse_out = parse_botanical_taxon_author_citation(
            rec['taxonAuthorCitation']
        )
        if parse_out:
            auth_parsed[rec['item']] = parse_out
    auths = []
    for item in auth_parsed:
        auths.extend(auth_parsed[item]['auth'])
        if 'ex_auth' in auth_parsed[item]:
            auths.extend(auth_parsed[item]['ex_auth'])
    auths = list(set(auths))
    print(f"{str(len(auths))} distinct author name abbreviations found")
    auth2qid = get_items_from_botanical_author_citation(auths)
    print(f"of which {str(len(auth2qid))} author names could be linked to Wikidata items")
    notfound = [i for i in auths if i not in auth2qid]
    if notfound:
        print("The following author name abbreviations could not be found in Wikidata:")
        print(" | ".join(notfound))
    # output quickstatements
    qs_auths = [['qid','P225','qal405','#'],] # initialize with header
    qs_exauths = [['qid','P225','qal697','#'],] # initialize with header
    for rec in o:
        authors = parse_botanical_taxon_author_citation(rec['taxonAuthorCitation'])
        if authors and 'auth' in authors:
            for a in authors['auth']:
                if a in auth2qid:
                    qs_auths.append([ rec['item'], '"""'+rec['taxonName']+'"""', auth2qid[a], 'matched from taxon author citation string'])
        if authors and 'ex_auth' in authors:
            for a in authors['ex_auth']:
                if a in auth2qid:
                    qs_exauths.append([ rec['item'], '"""'+rec['taxonName']+'"""', auth2qid[a], 'matched from taxon author citation string'])
    with open(f"add_taxon_author_{highertaxon_qid}.csv", "w") as fh:
        for line in qs_auths:
            fh.write(",".join(line))
            fh.write("\n")
    print(f"Output written to add_taxon_author_{highertaxon_qid}.csv")
    if len(qs_exauths) > 1:
        with open(f"add_ex_taxon_author_{highertaxon_qid}.csv", "w") as fh:
            for line in qs_exauths:
                fh.write(",".join(line))
                fh.write("\n")
        print(f"Output written to add_ex_taxon_author_{highertaxon_qid}.csv")
