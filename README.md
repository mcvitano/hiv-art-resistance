# Parse and Compile HIV Resistance Reports

Project to parse 10,000+ PDFs containing laboratory test results for resistance to
HIV antiretroviral treatments. Genotypic tests return position-specific mutations;
in a separate step these are translated to resistance scores using the Stanford HIVdb.
Phenotypic data, including fold change, replication capacity, and IC50 is extracted and
compiled in two formats; test order info is returned as one row per test order accession,
test results are returned in entity-attribute-value (EAV) format with one row per drug.
*No Data Included*