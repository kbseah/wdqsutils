Generate Wikidata QuickStatements for housekeeping tasks
========================================================

Scripts to generate batch edits of Wikidata for the following:

 - [x] Add missing descriptions for taxa in groups of interest
 - [x] Add missing descriptions for scholarly articles
 - [ ] Add missing labels for taxa from corresponding Wikipedia articles in target language
 - [x] Add missing identifiers for taxa from IRMNG database, using higher taxonomy to skip homonyms
 - [x] Add missing identifiers for taxa from GBIF database, using higher taxonomy to skip homonyms
 - [x] Add missing identifiers for fungal taxa from Index Fungorum
 - [ ] Add year of taxon publication for taxon names with reference where reference has role first valid description
 - [x] Link botanical authors as taxon author qualifiers by parsing taxon author citation strings
 - [ ] Find items where some but not all authors in the taxon author citation are linked as taxon author qualifiers


Notes
-----

IRMNG
 * API documentation: https://www.irmng.org/rest/

GBIF
 * API documentation: https://www.gbif.org/developer/species
 * Species match does not restrict hits to the higher taxon supplied, even in 'strict' mode.

Index Fungorum
 * API documentation: https://www.indexfungorum.org/ixfwebservice/fungus.asmx
 * Doesn't return higher taxonomy, can't easily disambiguate homonyms
