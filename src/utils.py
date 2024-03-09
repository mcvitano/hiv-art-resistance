
# https://hivinfo.nih.gov/understanding-hiv/fact-sheets/fda-approved-hiv-medicines
# https://docs.python.org/3/library/re.html#matching-vs-searching.

arv_master_dict = {
    # ... block RT; required for replication
    'NRTI': [('Ziagen', 'Abacavir', 'ABC', '1998-12-17'),
             ('Emtriva', 'Emtricitabine', 'FTC', '2003-07-02'),
             ('Epivir', 'Lamivudine', '3TC', '1995-11-17'),
             ('Viread', 'Tenofovir', 'TFV', '2001-10-26'),
             ('Vemlidy', 'Tenofovir', 'TAF', '2015-11-05'),
             ('Retrovir', 'Zidovudine', 'ZDV', '1987-03-19'),
             ('Videx', 'Didanosine', 'ddI', '1991-10'),
             ('Hivid', 'Zalcitabine', 'ddC', '1992-06'),
             ('Zerit', 'Stavudine', 'd4T', '1994')],
    # ... alter RT; required for replication
    'NNRTI':  [('Pifeltro', 'Doravirine', 'DOR', '2018-08-30'),
               ('Sustiva', 'Efavirenz', 'EFV', '1998-09-17'),
               ('Intelence', 'Etravirine', 'ETR', '2008-01-18'),
               ('Viramune', 'Nevirapine', 'NVP', '1996-06-21'),
               ('Edurant', 'Rilpivirine', 'RPV', '2011-05-20'),
               ('Rescriptor', 'Delavirdine', 'DLV', 'Unknown')],
    # ... block integrase; required for replication
    'INSTI': [('Bictegravir', 'Bictegravir', 'BIC', '2018-02-07'),
              ('Vitekta', 'Elvitegravir', 'EVG', '2012-08-27'),
              ('Vocabria', 'Cabotegravir', 'CAB', '2021-01-22'),
              ('Isentress', 'Raltegravir', 'RAL', '2007-10-12'),
              ('Tivicay', 'Dolutegravir', 'DTG', '2013-08-13')],
    # ... block protease; required for replication
    # ... prevent processing of viral gag and gag -pol polyprotein precursors,
    # ... result in immature non-infectious viral particles
    'PI': [('Reyataz', 'Atazanavir', 'ATV', '2003-06-20'),
           ('Prezista', 'Darunavir', 'DRV', '2006-06-23'),
           ('Lexiva', 'Fosamprenavir', 'AMP', '2003-10-20'),
           ('Agenerase', 'Amprenavir', 'AMP', '1999-04-15'),
           ('Crixivan', 'Indinavir', 'IDV', 'Unknown'),
           ('Kaletra', 'Lopinavir', 'LPV', 'Unknown'),
           ('Viracept', 'Nelfinavir', 'NFV', 'Unknown'),
           ('Fortovase', 'Saquinavir', 'SQV', 'Unknown'),
           ('Invirase', 'Saquinavir', 'SQV', 'Unknown'),
           ('Norvir', 'Ritonavir', 'RTV', '1996-03-01'),
           ('Aptivus', 'Tipranavir', 'TPV', '2005-06-22')],
    #
    'FUSION': [('Fuzeon', 'Enfuvirtide', 'ENF', '2003-03-13'),]
}


def extract_from_zips(path_to_zips):
    """
    Obtains paths to all files within .zip folders in the specified directory.

    Unzips all of the .zip folders in the specified directory
    to a folder of the name "Reports_[extraction_date]"
    where extraction_date is a descriptive string -- such as 30JUN2019.
    It then returns a list of paths to these files.

    Parameters
    ----------
    path_to_zips : PosixPath

    extraction_date : PosixPath

    Returns
    -------
    List of Posix paths
    """

    from zipfile import ZipFile
    from ipypb import track
    from pathlib import Path

    path_to_zips = Path(path_to_zips)
    output_path = path_to_zips.joinpath('extracted-reports')

    # list all .zip files in folder
    zip_files = [i for i in path_to_zips.iterdir() if ".zip" in str(i)]

    # if folder for these ZIP's already exists
    if output_path.exists():
        # return a list of output report paths
        report_paths = [i for i in output_path.iterdir() if ".PDF" in str(i)]
        # remove duplicates
        report_paths = list(set(report_paths))
        return report_paths

    else:
        for i in track(range(0, len(zip_files)), label='Zip Files'):
            # unzip all PDF reports to a 'temp' folder
            with ZipFile(zip_files[i], 'r') as zipObj:
                # extract the contents into a different directory
                zipObj.extractall(output_path)

        # return the report paths
        report_paths = [i for i in output_path.iterdir() if ".PDF" in str(i)]
        # remove duplicates
        report_paths = list(set(report_paths))
        return report_paths


def scrape_PDF_to_text(path_to_pdf):
    """
    Scrapes a single PDF to text, maintaining relative position on page.

    Parameters
    ----------
    path_to_pdf : PosixPath

    Returns
    -------
    A text version of the specified PDF
    """

    from pdfminer3.converter import TextConverter
    # from pdfminer3.layout import LAParams, LTTextBox
    from pdfminer3.pdfpage import PDFPage
    from pdfminer3.pdfinterp import PDFResourceManager
    from pdfminer3.pdfinterp import PDFPageInterpreter
    # from pdfminer3.converter import PDFPageAggregator
    import io

    resource_manager = PDFResourceManager()
    fake_file_handle = io.StringIO()
    converter = TextConverter(resource_manager, fake_file_handle)
    page_interpreter = PDFPageInterpreter(resource_manager, converter)

    with open(path_to_pdf, 'rb') as fh:

        for page in PDFPage.get_pages(fh,
                                      caching=True,
                                      check_extractable=True):
            page_interpreter.process_page(page)

        text = fake_file_handle.getvalue()

    # close open handles
    converter.close()
    fake_file_handle.close()

    # remove symbols
    text = text.replace('§', '')

    return text


def add_chart_reviewed_mrns(df, path_to_chart_review_csv):
    from pathlib import Path
    import pandas as pd
    import numpy as np

    # Query to obtain file for chart review
    # df.loc[(df.PAT_MRN_ID.str.startswith('00', na=False)) | (df.PAT_MRN_ID.isnull(),
    #       ['DOC_NAME', 'FULL_NAME', 'PAT_MRN_ID', 'SSN', 'BIRTH_DATE', 'GENDER']]\
    #       .to_csv(monogram_path.joinpath('missing-mrn-for-chart-review.csv'))

    chart_review = pd.read_csv(Path(path_to_chart_review_csv),
                               dtype={'PAT_MRN_ID': str}, sep=',')

    # MRN can be up to 8 characters (if not starting with '0')
    #   ... but if < 7 it is zero-padded to 7 characters
    # Will need to perform manual chart review for missing MRNs
    mrn_length = chart_review['PAT_MRN_ID'].str.len()
    leading_zero = chart_review['PAT_MRN_ID'].str.startswith('0')

    chart_review.loc[(mrn_length > 8) & (leading_zero), 
                      'MRN_OUTDATED'] = chart_review['PAT_MRN_ID'].str.slice(1, )
    chart_review.loc[(mrn_length > 8), 'PAT_MRN_ID'] = np.NaN

    chart_review.loc[(mrn_length == 8) & (leading_zero), 
                     'PAT_MRN_ID'] = chart_review['PAT_MRN_ID'].str.slice(1, )
    chart_review.loc[(mrn_length == 8), 'MRN_OUTDATED'] = 1

    chart_review.loc[mrn_length < 7, 
                     'PAT_MRN_ID'] = chart_review['PAT_MRN_ID'].str.zfill(7)

    for i, row in chart_review.iterrows():
        df.loc[df['DOC_NAME'] == row['DOC_NAME'], 'PAT_MRN_ID'] = row['PAT_MRN_ID']
        df.loc[df['DOC_NAME'] == row['DOC_NAME'], 'MRN_CHART_REVIEWED'] = 1

    return df


def load_hivdb_scores(path_to_hivdb, loci, rule_type):
    from pathlib import Path
    import pandas as pd

    hivdb = Path(path_to_hivdb)
    loci = loci.upper()
    rule_type = rule_type.upper()

    if loci.upper() not in ['RT', 'PI', 'PR', 'INSTI']:
        raise ValueError('loci argument must be one of [RT, PI/PR, INSTI]')

    if rule_type.upper() not in ['SIMPLE', 'COMPLEX']:
        raise ValueError('rule_type argument must be one of [Simple, Complex]')

    try:
        if loci == 'RT':
            nrti_path = [i for i in hivdb.glob(f'nrti-scores-{rule_type.lower()}*')][0]
            nnrti_path = [i for i in hivdb.glob(f'nnrti-scores-{rule_type.lower()}*')][0]

            nrti_scores = pd.read_csv(nrti_path, sep=',')
            nrti_scores['RESISTANCE_TYPE'] = 'NRTI'

            nnrti_scores = pd.read_csv(nnrti_path, sep=',')
            nnrti_scores['RESISTANCE_TYPE'] = 'NNRTI'

            scores = nrti_scores.merge(nnrti_scores, how='outer')

        elif loci in ['PI', 'PR']:
            pi_path = [i for i in hivdb.glob(f'pi-scores-{rule_type.lower()}*')][0]
            scores = pd.read_csv(pi_path, sep=',')
            scores['RESISTANCE_TYPE'] = 'PI'

        # loci == 'INSTI'
        else:
            insti_path = [i for i in hivdb.glob(f'insti-scores-{rule_type.lower()}*')][0]
            scores = pd.read_csv(insti_path, sep=',')
            scores['RESISTANCE_TYPE'] = 'INSTI'

        # standardize to uppercase names
        scores.columns = [i.upper().replace(' ', '_') for i in scores.columns]

        # For "simple" scores, extract Position and Amino Acid
        if not 'COMBINATION_RULE' in scores.columns:
            scores['POSITION'] = scores['RULE'].str.extract('([\d]+)')
            scores['POSITION'] = scores['POSITION'].astype(float)
            scores['AA'] = scores['RULE'].str.extract('[\d]+([A-Z/\^]*)')

        # Rename
        scores.columns = [i.replace('COMBINATION_', '') for i in scores.columns]

        # reset the index (required for iterating and modifying in place)
        scores.reset_index(drop=True, inplace=True)

        # RULE                POSITION     AA     BIC     DTG     ...
        # -----------------------------------------------------------
        # G118R + E138AKT       <null>    <null>  10      10      ...
        # E138AKT + G140ACS     <null>    <null>  10      10      ...

    except FileNotFoundError:
        print("Score files could not be located.")
        print("Check that the specified directory contains files named 'ScoresNRTI*', etc.")

    return scores


def load_hivdb_comments(path_to_hivdb, loci):
    from pathlib import Path
    import pandas as pd

    hivdb = Path(path_to_hivdb)
    loci = loci.upper()

    if loci not in ['RT', 'PI', 'PR', 'INSTI']:
        raise ValueError('--loci argument must be one of [RT, PI/PR, INSTI]')

    try:
        if loci == 'RT':
            nrti_path = [i for i in hivdb.glob('nrti-comments*')][0]
            nnrti_path = [i for i in hivdb.glob('nnrti-comments*')][0]

            nrti_comments = pd.read_csv(nrti_path, sep=',', skiprows=1)
            nrti_comments.columns = ['MUTATION', 'MUTATION_TYPE', 'COMMENT']
            nrti_comments['POSITION'] = nrti_comments['MUTATION'].str.extract('([\d]+)')
            nrti_comments['AA'] = nrti_comments['MUTATION'].str.extract('[\d]+([A-Z/\^]*)')
            nrti_comments.loc[
                nrti_comments['MUTATION_TYPE'].isnull(),
                'MUTATION_TYPE'] = 'Predicted'
            nrti_comments.drop(columns='MUTATION', inplace=True)

            nnrti_comments = pd.read_csv(nnrti_path, sep=',', skiprows=1)
            nnrti_comments.columns = ['MUTATION', 'MUTATION_TYPE', 'COMMENT']
            nnrti_comments['POSITION'] = nnrti_comments['MUTATION'].str.extract('([\d]+)')
            nnrti_comments['AA'] = nnrti_comments['MUTATION'].str.extract('[\d]+([A-Z/\^]*)')
            nnrti_comments.loc[
                nnrti_comments['MUTATION_TYPE'].isnull(),
                'MUTATION_TYPE'] = 'Predicted'
            nnrti_comments.drop(columns='MUTATION', inplace=True)

            comments = nrti_comments.merge(nnrti_comments, how='outer')

        elif loci in ['PI', 'PR']:
            pi_path = [i for i in hivdb.glob('pi-comments*')][0]
            comments = pd.read_csv(pi_path, sep=',', skiprows=1)
            comments.columns = ['MUTATION', 'MUTATION_TYPE', 'COMMENT']
            comments['POSITION'] = comments['MUTATION'].str.extract('([\d]+)')
            comments['AA'] = comments['MUTATION'].str.extract('[\d]+([A-Z/\^]*)')
            comments.loc[
                comments['MUTATION_TYPE'].isnull(),
                'MUTATION_TYPE'] = 'Predicted'
            comments.drop(columns='MUTATION', inplace=True)

        # loci == 'INSTI'
        else:
            insti_path = [i for i in hivdb.glob('insti-comments*')][0]
            comments = pd.read_csv(insti_path, sep=',', skiprows=1)
            comments.columns = ['MUTATION', 'MUTATION_TYPE', 'COMMENT']
            comments['POSITION'] = comments['MUTATION'].str.extract('([\d]+)')
            comments['AA'] = comments['MUTATION'].str.extract('[\d]+([A-Z/\^]*)')
            comments.loc[
                comments['MUTATION_TYPE'].isnull(),
                'MUTATION_TYPE'] = 'Predicted'
            comments.drop(columns='MUTATION', inplace=True)

        # standardize to uppercase names
        comments.columns = [i.upper().replace(' ', '_') for i in comments.columns]

        # align datatypes (future step merges with a string-type POSITION field)
        comments['POSITION'] = comments['POSITION'].astype(float)

        # POSITION     AA     MUTATION_TYPE     COMMENT
        # ---------------------------------------------
        #   115        F          NRTI          Y115F causes intermediate resistance to ABC an...
        #   116        Y          NRTI          F116Y usually occurs in combination with the m...

    except FileNotFoundError:
        print("Comment files could not be located.")
        print("Check that the specified directory contains files named 'CommentsNRTI*', etc.")

    return comments


def replicate_comments_over_amino_acids(comments):
    """
    Transforms the original dataframe:
        Position     AA     Mutation Type     Comment
        ---------------------------------------------
        181          C       NNRTI            Y181C is a non-polymorphic mutation selected i...
        181          FSG     NNRTI            Y181F/S/G are rare non-polymorphic NNRTI-assoc...
        181          IV      NNRTI            Y181I/V are 2-base pair non-polymorphic mutati...

    into...
        Position     AA     Mutation Type     Comment
        ---------------------------------------------
        181          C       NNRTI            Y181C is a non-polymorphic mutation selected i...
        181          F       NNRTI            Y181F/S/G are rare non-polymorphic NNRTI-assoc...
        181          S       NNRTI            Y181F/S/G are rare non-polymorphic NNRTI-assoc...
        181          G       NNRTI            Y181F/S/G are rare non-polymorphic NNRTI-assoc...
        181          I       NNRTI            Y181I/V are 2-base pair non-polymorphic mutati...
        181          V       NNRTI            Y181I/V are 2-base pair non-polymorphic mutati...
    """
    import pandas as pd
    import numpy as np
    import re

    # split off insertions/deletions
    ins_del = comments[comments['AA'].str.contains('Insertion|Deletion', na=False)]
    comments = comments[~comments['AA'].str.contains('Insertion|Deletion', na=False)]

    # replicate each row by the number of amino acids accounted for
    # [KAT] => [KAT]
    #          [KAT]
    #          [KAT]
    reps = comments['AA'].str.len()
    comments = comments.loc[np.repeat(comments.index.values, reps)]
    comments['aa_cum'] = comments.groupby(['POSITION', 'AA']).cumcount()

    # [KAT] => [K]
    # [KAT]    [A]
    # [KAT]    [T]
    aas = []
    for i, row in comments.iterrows():
        aas.append(re.search('[A-Z]'*(row['aa_cum']) + '([A-Z])', row['AA']).group(1).strip())

    comments['AA'] = aas

    comments.drop('aa_cum', axis=1, inplace=True)

    # merge back in the insertions/deletions
    comments = pd.concat([comments, ins_del])

    return comments


def parse_genotypic_reports(path_to_zips, test_type):
    from src.utils import extract_from_zips, scrape_PDF_to_text
    from ipypb import track
    import pandas as pd
    import numpy as np
    import warnings

    def _mark_report_as_complete(sample_text):
        import re

        report_complete = True

        phrases = ['common causes of assay failure',
                'unable to perform testing',
                'quantity not sufficient',
                'inadequate specimen volume',
                'low sample volume',
                'is not ready to report',
                'canceled per client',
                'insufficient HIV-infected cells',
                'duplicate order',
                'could not be obtained',
                'could not be completed',
                'this sample should not be used',
                'inappropriately collected',
                'resubmit a new specimen',
                'resubmit a new sample',
                'unsuccessful testing of this sample'
                ]

        for phrase in phrases:
            if report_complete:
                if re.search(phrase, sample_text, flags=re.IGNORECASE):
                    report_complete = False
            else:
                pass

        return report_complete

    def _extract_order_info_as_dict(sample_text: str) -> dict:
        """
        Extract order info from text text of parsed PDF report.
        The data elements always follow the same ordering though
        some may be missing.

        Parameters
        ----------
        sample_text : str
            Text from parsed PDF report

        Returns
        ----------
        row_values : list
            List of values; one for each order info component
        """
        import re
        import numpy as np

        # Get phrasing used for MRN
        if re.search('Medical Record #', sample_text):
            mrn_prefix = 'Medical Record #'
        else:
            mrn_prefix = 'Patient ID'

        # Get phrasing used for Lab Order ID
        if re.search("ID/Order #", sample_text):
            prov_suffix = 'Reference Lab ID/Order #'
        else:
            prov_suffix = 'Reference Lab ID'

        full_name = re.search(
            'Patient Name[:\s]*([A-Za-z -/.]*?)(?=DOB)', sample_text)
        birth_date = re.search(
            'DOB[:\s]*([A-Za-z0-9-/]*?)[ ]*(?=Patient ID)', sample_text)
        mrn = re.search(
            f'{mrn_prefix}[:\s]*([\d]*?)[ \D]*(?=Gender)', sample_text)

        # occassionally Monogram placed the SSN in place of the PAT_MRN_ID
        ssn = re.search(
            f'{mrn_prefix}[:\s]*(\d\d\d-\d\d-\d\d\d\d)[ \D]*(?=Gender)',
            sample_text)
        if ssn:
            mrn = None

        gender = re.search(
            'Gender[:\s]*([A-Za-z]*?)[ ]*(?=Monogram)', sample_text)
        test_accession = re.search(
            'Accession[:\s#]*([A-Za-z0-9-/_]*?)[ ]*(?=Date)', sample_text)
        # Dates follow the format:  17-MAY-2016 11:20 PT
        collection_date = re.search(
            'Date Collected[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        received_date = re.search(
            'Date Received[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        reported_date = re.search(
            'Date Reported[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        # Mode in [F, M, W]
        test_mode = re.search(
            'Mode[:\s]*([A-Z,]*?)[ ]*(?=Report)', sample_text)
        report_status = re.search(
            'Report Status[:\s]*([A-Z]*?)[ ]*(?=Referring)', sample_text)
        # Referring Physician
        referring_prov = re.search(
            f'Referring Physician[:\s]*([A-Za-z\s\d,]*?)(?={prov_suffix})',
            sample_text)
        # keep only first two names (do not keep address, if present)
        if referring_prov:
            referring_prov = re.search(
                '([A-Za-z]*[ ][A-Za-z]*)', referring_prov.group(1))
        # Order ID should not contain letters
        # ... in the early days the hospital floor was substituted
        order_id = re.search(
            f'{prov_suffix}[:\s]*([0-9]*?)[ _A-Z-:]', sample_text)

        # HIV-1 subtype should be present unless viral load is < 500 copies/mL
        #   or the specimen fails to meet collection specifications
        hiv1_subtype = re.search(
            '1[ -]?Sub[-]?type:[ ]+([A-Za-z\d\s]*?)[ ]*(?=Generic)',
            sample_text)
        # Algorithm version is a canned-comment
        #   posted regardless of whether testing was performed
        #   (not used by Phenosense-GT)
        algorithm = re.search(
            'proprietary algorithm \(version[ #]([0-9]*)\)', sample_text)

        val_list = [full_name, birth_date, mrn, ssn, gender, test_accession,
                    collection_date, received_date, reported_date, test_mode,
                    report_status, referring_prov, order_id, hiv1_subtype,
                    algorithm]
        row_values = [v.group(1).strip()
                      .title() if v else np.NaN for v in val_list]

        col_names = ['FULL_NAME', 'BIRTH_DATE', 'PAT_MRN_ID', 'SSN', 'GENDER',
                     'ACCESSION', 'COLLECTED_DATE', 'RECEIVED_DATE',
                     'REPORTED_DATE', 'MODE', 'REPORT_STATUS',
                     'REFERRING_PROV', 'ORDER_ID', 'HIV1_SUBTYPE',
                     'ALGORITHM_VERSION']

        order_info_dict = dict(zip(col_names, row_values))

        return order_info_dict

    def _extract_drms_as_dict(sample_text: str) -> dict:
        from src.utils import arv_master_dict
        import re

        #arv_resistance_dict = {}
        #for art_class, art_list in arv_master_dict.items():
        #    for art_attributes in art_list:
        #        resistance = re.search(
        #            f'{art_attributes[0]}[ ]+([A-Za-z\d\s,]+)[ ]+(?={art_attributes[2]})', sample_text)
        #        if resistance:
        #            arv_resistance_dict[art_attributes[2]] = resistance.group(1).strip()

        #        resistance_boosted = re.search(
        #            f'{art_attributes[0]}[ ]\/[ ]r[ ]+([A-Za-z\d\s,]+)[ ]+(?={art_attributes[2]})', sample_text)
        #        if resistance_boosted:
        #            arv_resistance_dict[f'{art_attributes[2]}_r'] = resistance_boosted.group(1).strip()

        arv_resistance_dict = {}
        for art_class, art_list in arv_master_dict.items():
            # {'NRTI': [('Ziagen', 'Abacavir', 'ABC', '1998-12-17'), ...],
            #  'NNRTI': [('Pifeltro', 'Doravirine', 'DOR', '2018-08-30'), ...], ...}
            for art_attributes in art_list:
                # If brand name == generic name
                # e.g., Bictegravir BictegravirL74I  BIC
                if art_attributes[0] == art_attributes[1]:
                    resistance = re.search(
                        f'{art_attributes[0]}[ ]+{art_attributes[1]}[ ]*([A-Za-z\d\s,/]+)[ ]+?(?={art_attributes[2]})', sample_text)
                    if resistance:
                        arv_resistance_dict[art_attributes[2]] = resistance.group(1).strip()

                # All others (brand name != generic name)
                # e.g., Dolutegravir TivicayL74I  DTG
                else:  
                    resistance = re.search(
                        f'{art_attributes[0]}[ ]*([A-Za-z\d\s,/]+)[ ]+?(?={art_attributes[2]})', sample_text)
                    if resistance:
                        # Because the above regex must include the '/' in order to capture
                        #   mixtures (e.g., Q58Q/E), there is no way to exclude the '/ r'
                        #   characters from the match.
                        # If these characters are found, set the entire match to NULL
                        #   and allow the next 'boosted' search to capture the mutations
                        if re.search('/ r', sample_text):
                            arv_resistance_dict[art_attributes[2]] = np.NaN
                        else:
                            arv_resistance_dict[art_attributes[2]] = resistance.group(1).strip()

                    # Boosted ARVs include '/r' after brand name (and abbreviation)
                    resistance_boosted = re.search(
                        f'{art_attributes[0]}[ ]*\/[ ]r[ ]*([A-Za-z\d\s,/]+)[ ]+?(?={art_attributes[2]})', sample_text)
                    if resistance_boosted:
                        arv_resistance_dict[f'{art_attributes[2]}_r'] = resistance_boosted.group(1).strip()

        return arv_resistance_dict

    def _extract_full_mutation_lists_as_dict(sample_text: str,
                                             test_type: str) -> dict:
        import re

        # To avoid writing an entirely separate function solely for parsing
        #   Phenosense-GT tests the following switch is used.
        #   The only difference between parsing the mutation list for
        #   Phenosense-GT tests vs.
        #   [Geneseq, Genosure-MG, Genosure-PRiME, Genosure-Archive]
        #   is the use of a colon (:) rather than a space (\s).
        if test_type.upper() in ['PHENOSENSE-GT']:
            q = ':'
        else:
            q = '\s'

        # regex for convenience
        alphanum_list = '[A-Z0-9,/ \^]*?'

        # REVERSE TRANSCRIPTASE
        # ---------------------
        # Must account for the integrase loci section in (prime, archive)
        if re.search(f' RT{q}[ ]?None', sample_text):
            rt_list = 'None'

        else:
            rt_list = None
            for behind in [f'(?<= RT{q})']:
                for ahead in [f'(?= PR{q})', f'(?= PI{q})',
                              f'(?= IN{q})', '[ ]+[A-Z][a-z][a-z]']:
                    if not rt_list:
                        try:
                            rt_list = re.search(
                                f'{behind}({alphanum_list}){ahead}',
                                sample_text)
                            rt_list = rt_list.group(1).strip().replace(':', '')
                        except AttributeError:
                            pass

        # INTEGRASE
        # ---------
        # Integrase section is only present for archive/prime tests
        # Occurs just prior to the list of protease mutations
        if re.search(f' IN{q}[ ]?None', sample_text):
            insti_list = 'None'

        else:
            insti_list = None
            for behind in [f'(?<= IN{q})']:
                for ahead in [f'(?= PR{q})', f'(?= PI{q})',
                              '[ ]+[A-Z][a-z][a-z]']:
                    if not insti_list:
                        try:
                            insti_list = re.search(
                                f'{behind}({alphanum_list}){ahead}',
                                sample_text)
                            insti_list = insti_list.group(1).strip()\
                                                   .replace(':', '')
                        except AttributeError:
                            pass

        # PROTEASE
        # ---------
        # Same for all test-types
        if re.search(f' PR{q}[ ]?None', sample_text):
            pr_list = 'None'

        else:
            pr_list = None
            for behind in [f'(?<= PR{q})', f'(?<= PI{q})']:
                for ahead in [f'(?= IN{q})', '[ ]+[A-Z][a-z][a-z]']:
                    if not pr_list:
                        try:
                            pr_list = re.search(
                                f'{behind}({alphanum_list}){ahead}',
                                sample_text)
                            pr_list = pr_list.group(1).strip().replace(':', '')
                        except AttributeError:
                            pass

        mutations_dict = {}
        mutations_dict['RT_LIST'] = rt_list
        mutations_dict['INSTI_LIST'] = insti_list
        mutations_dict['PR_LIST'] = pr_list

        return mutations_dict

    def _reorder_dataframe_columns(df) -> pd.DataFrame:
        # get list
        cols = df.columns.to_list()

        # order info
        order_info = ['DOC_NAME', 'TEST_TYPE',
                      'REPORT_COMPLETE',
                      'FULL_NAME', 'PAT_MRN_ID', 'SSN',
                      'BIRTH_DATE', 'GENDER', 'ACCESSION', 'ORDER_ID',
                      'REFERRING_PROV', 'COLLECTED_DATE', 'RECEIVED_DATE',
                      'REPORTED_DATE', 'REPORT_STATUS', 'MODE',
                      'ALGORITHM_VERSION',  'HIV1_SUBTYPE']

        order_info = [i for i in order_info if i in cols]

        # lists of all mutations by loci
        mutation_list_cols = [i for i in cols if i.endswith('_list')]

        # individual mutations
        nrti = ['3TC', 'ABC', 'd4T', 'ddC', 'ddI', 'FTC', 'TFV', 'TAF', 'ZDV']
        nnrti = ['DOR', 'DLV', 'EFV', 'ETR', 'NVP', 'RPV']
        insti = ['BIC', 'CAB', 'DTG', 'EVG', 'RAL']
        pi = ['AMP', 'AMPr', 'ATV', 'ATVr', 'DRV', 'DRVr', 'IDV', 'IDVr',
              'LPV', 'LPVr', 'NFV', 'NFVr', 'RTV', 'RTVr', 'SQV', 'SQVr',
              'TPV', 'TPVr']
        fusion = ['ENF']

        nrti_cols = [i for i in nrti if i in cols]
        nnrti_cols = [i for i in nnrti if i in cols]
        insti_cols = [i for i in insti if i in cols]
        pi_cols = [i for i in pi if i in cols]
        fusion_cols = [i for i in fusion if i in cols]

        # create re-ordered list of columns
        cols_reordered = order_info + mutation_list_cols + nrti_cols + nnrti_cols + pi_cols + insti_cols + fusion_cols
        not_listed_error = [i for i in cols if i not in cols_reordered]

        # apply it
        df = df[cols_reordered + not_listed_error]

        return df

    # Future Warning: Setting an item of incompatible dtype is deprecated
    # This appears to be a bug in pandas 2.1.1
    # See https://github.com/pandas-dev/pandas/issues/55025
    warnings.simplefilter(action='ignore', category=FutureWarning)

    allowable_test_types = ['GENESEQ', 'GENOSURE-MG', 'GENOSURE-PRIME',
                            'GENOSURE-ARCHIVE', 'PHENOSENSE-GT']
    if test_type.upper() not in allowable_test_types:
        raise ValueError(f'Invalid test type ({allowable_test_types})')

    # Extract PDF reports from Zip files
    report_paths = extract_from_zips(path_to_zips)

    # Empty dataframe (to fill)
    # Many single-row insertions
    df = pd.DataFrame()

    # Loop over PDF's
    for i in track(range(0, len(report_paths)), label='Records'):

        # Parse PDF to text
        sample_text = scrape_PDF_to_text(report_paths[i])

        # Extract order info
        order_info_dict = _extract_order_info_as_dict(sample_text=sample_text)

        # Insert order info into dataframe
        df.loc[i, 'TEST_TYPE'] = test_type.upper()
        df.loc[i, 'DOC_NAME'] = report_paths[i].name

        for attribute, value in order_info_dict.items():
            df.loc[i, attribute] = value

        # If report is not marked "incomplete"
        #   gather oberserved mutations
        report_complete = _mark_report_as_complete(sample_text=sample_text)
        df.loc[i, 'REPORT_COMPLETE'] = report_complete

        if report_complete:
            # Phenosense-GT tests do *not* have mutations listed at the
            #   drug-level (only lists at the loci-level)
            if test_type.upper() not in ['PHENOSENSE-GT']:
                arv_resistance_dict = _extract_drms_as_dict(
                    sample_text=sample_text)

                # Insert observed mutations into dataframe
                for arv, resistance_result in arv_resistance_dict.items():
                    df.loc[i, arv] = resistance_result

            # Loci-specific lists of mutations
            mutations_list_dict = _extract_full_mutation_lists_as_dict(
                sample_text=sample_text, test_type=test_type)

            # Insert observed mutations into dataframe
            for loci, mutations_list in mutations_list_dict.items():
                df.loc[i, loci] = mutations_list

        # Reorder columns (aesthetic)
        df = _reorder_dataframe_columns(df=df)

    # Re-sort by patient
    df.sort_values(['PAT_MRN_ID', 'ACCESSION'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Set '' (artefact of regular expression) to missing
    for col in [i for i in df.columns if df[i].dtype == 'O']:
        df.loc[df[col] == '', col] = np.NaN

    # Dates
    for date_col in ['BIRTH_DATE', 'COLLECTED_DATE',
                     'RECEIVED_DATE', 'REPORTED_DATE']:
        df[date_col] = pd.to_datetime(df[date_col], format='mixed')

    # MRN can be up to 8 characters (if not starting with '0')
    #   ... but if < 7 it is zero-padded to 7 characters
    # Will need to perform manual chart review for missing MRNs
    mrn_length = df['PAT_MRN_ID'].str.len()
    leading_zero = df['PAT_MRN_ID'].str.startswith('0')

    df.loc[(mrn_length > 8) & (leading_zero), 
           'MRN_OUTDATED'] = df['PAT_MRN_ID'].str.slice(1, )
    df.loc[(mrn_length > 8), 'PAT_MRN_ID'] = np.NaN

    df.loc[(mrn_length == 8) & (leading_zero),
           'PAT_MRN_ID'] = df['PAT_MRN_ID'].str.slice(1, )
    df.loc[(mrn_length == 8), 'MRN_OUTDATED'] = 1

    df.loc[mrn_length < 7, 'PAT_MRN_ID'] = df['PAT_MRN_ID'].str.zfill(7)

    # Manual fix (no info on PDF)
    df.loc[df['DOC_NAME'] == '96063822_18-184109-1GN-0_F.PDF',
           'ACCESSION'] = '18-184109'

    print(f"{i+1} {test_type} records parsed")

    return df


def melt_loci_specific_mutations_to_position(mutations_df, loci):
    import pandas as pd
    import numpy as np
    import re
    from collections import OrderedDict as o

    from warnings import simplefilter

    # Future Warning: Setting an item of incompatible dtype is deprecated
    # This appears to be a bug in pandas 2.1.1
    # See https://github.com/pandas-dev/pandas/issues/55025
    simplefilter(action='ignore', category=FutureWarning)

    # PerformanceWarning: DataFrame is highly fragmented.
    # This new to pandas 2.0(+)
    # See https://stackoverflow.com/questions/68292862
    simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

    loci = loci.upper()
    if loci.upper() not in ['RT', 'PI', 'PR', 'INSTI']:
        raise ValueError('Loci argument must be one of [RT, PI/PR, INSTI]')

    loci_list = loci + '_LIST'
    if loci_list not in mutations_df.columns:
        raise ValueError(f'DataFrame has no {loci_list} field.')

    # Make a copy of the mutations dataframe (one list per test/row)
    df = mutations_df.copy()

    print(f'Extracting positions of mutations within the {loci_list} locus.')
    print('All values of "None" must be set to missing to allow ')
    print('     filling mutations forward over time (within patient).')
    print()

    # Keep only
    #   (1) completed tests
    #   (2) with a medical record number
    #   (3) non-null list of loci-specific mutations (e.g., RT_list)
    print('Dropped')

    have_mutations_list = (df[loci_list].notnull())
    print(f'  - {len(df[~have_mutations_list])} without observed mutations at the {loci} locus')
    df = df[have_mutations_list]

    complete_test = (df['REPORT_COMPLETE'] == True)
    print(f'  - {len(df[~complete_test])} incomplete test accessions')
    df = df[complete_test]

    have_mrn = (df['PAT_MRN_ID'].notnull())
    print(f'  - {len(df[~have_mrn])} test accessions with no MRN')
    df = df[have_mrn]
    print('')

    order_info_cols = ['PAT_MRN_ID', 'COLLECTED_DATE', 'ACCESSION']
    df = df[order_info_cols + [loci_list]]

    # Split off patients with a single test
    #   to reduce processing time in the step below,
    #   which is required only for patients with multiple tests
    have_multiple_tests = df \
        .groupby('PAT_MRN_ID')['ACCESSION'] \
        .transform('nunique') > 1

    single_test_df = df[~have_multiple_tests].copy()
    # alphabetize/standardize the {loci_list} components
    single_test_df[loci_list] = single_test_df[loci_list].apply(lambda x: ",".join(sorted(x.split(","))))

    # multiple_test_df = df[have_multiple_tests].copy()
    # Patients never actually "lose" a mutation; it's just unobserved due to 
    #   a combination of treatment-inhibition and between-strain-competition.
    # Therefore, we want to fill mutations over time, within patient.
    # However, mutations are currently in an unordered list.
    # In order to create a standardized order of mutations,
    #   we will first create columns from the entries in {loci-list}

    # Removed in favor of Ordered Dict approach
    # for i, row in multiple_test_df.iterrows():
    #    for mutation in row[loci_list].split(','):
    #        multiple_test_df.loc[i, mutation.strip()] = mutation.strip()
    # multiple_test_df.drop(columns=loci_list, inplace=True)

    # PAT_MRN_ID     COLLECTED_DT     ...     Q102K     C162S     T200T/A     R211K     V245E/K     M184M/V
    # -----------------------------------------------------------------------------------------------------
    #   0000000       2018-02-14      ...      NaN       NaN        NaN       R211K      NaN         NaN
    #   0000000       2018-02-14      ...      NaN       NaN        NaN       R211K      NaN        M184M/V

    # ... then we will fill forward over time (within patient)
    # multiple_test_df.sort_values(['PAT_MRN_ID', 'COLLECTED_DATE'], inplace=True)
    # mutation_dummy_cols = [i for i in multiple_test_df.columns if i not in order_info_cols]

    # multiple_test_df[mutation_dummy_cols] = multiple_test_df \
    #     .groupby('PAT_MRN_ID')[mutation_dummy_cols] \
    #     .transform(lambda x: x.ffill())
    # multiple_test_df.reset_index(drop=True, inplace=True)

    # ... and use the dummy columns to re-create the {loci-list} field
    #   in a standardized order (per patient, overtime)
    # for i, row in multiple_test_df.iterrows():
        # that are not null
    #    nonnull_mask = (row[mutation_dummy_cols].isnull() == False)
    #    # re-create
    #    multiple_test_df.loc[i, loci_list] = ','.join(row[mutation_dummy_cols][nonnull_mask].index.tolist()).strip()


    # Patients never actually "lose" a mutation; it's just unobserved due to 
    #   a combination of treatment-inhibition and between-strain-competition.
    # Therefore, we want to fill mutations over time, within patient.
    # However, mutations are currently in an unordered list.
    # Use an Ordered Dict{ } to standardize the order of mutations with the lists.
    multiple_test_df = df[have_multiple_tests].copy()
    multiple_test_df.sort_values(['PAT_MRN_ID', 'COLLECTED_DATE'], inplace=True)
    multiple_test_df.reset_index(inplace=True, drop=True)

    for i, row in multiple_test_df.iterrows():
        if (i > 0) and (row['PAT_MRN_ID'] == multiple_test_df.loc[i-1]['PAT_MRN_ID']):
            combined_list = multiple_test_df.loc[i-1][loci_list] + ',' + row[loci_list]
            multiple_test_df.loc[i, loci_list] = combined_list

    multiple_test_df.loc[:, loci_list] = [
        ', '.join(o.fromkeys(x.split(','), 1))
        for x in multiple_test_df[loci_list]]

    # Recombine dataframes
    position_df = pd.concat([single_test_df,
                            multiple_test_df[order_info_cols + [loci_list]]])
    position_df.reset_index(inplace=True, drop=True)

    # Pivot from wide to long (create mutation dummies)
    dummies_df = position_df[loci_list].str.split(",", expand=True)

    # 0	        1           2           ...
    # ------------------------------------------
    # V245V/M   D123E      np.NaN

    # Join (left) the dummies back into the position_df to get order info
    position_df = position_df[order_info_cols] \
        .merge(dummies_df, left_index=True, right_index=True, how='left')

    # Melt from wide to long
    # LINE is currently redundant with ACCESSION (but used in a later step)
    position_df = position_df.melt(id_vars=order_info_cols) \
        .sort_values(['PAT_MRN_ID', 'COLLECTED_DATE', 'variable']) \
        .rename(columns={'variable': 'LINE', 'value': 'MUTATION'})

    # remove any spaces at the begining of mutations (pre-cautinary)
    position_df['MUTATION'] = position_df['MUTATION'].str.strip()

    # Drop duplicates (artefact of filling mutations over time)
    position_df.drop_duplicates(subset=['ACCESSION', 'MUTATION'], inplace=True)

    # ACCESSION	    LINE	MUTATION
    # ------------------------------
    # 09-150972	    7	    V245V/M
    # 09-150972     8 	    D123E

    # Drop all null values generated during the melting process
    position_df = position_df.loc[position_df['MUTATION'].notnull()]

    # Extract the position and amino acid from mutations
    position_df['POSITION'] = position_df['MUTATION'].str.extract('[A-Z]([\d]+)')
    position_df['AA'] = position_df['MUTATION'].str.extract('[\d]+([A-Z/\^]*)')

    # ACCESSION	    LINE	MUTATION	POSITION    AA
    # ------------------------------------------------
    # 09-150972	    7	    V245V/M	    245         V/M
    # 09-150972     8 	    D123E	    123         E

    # Set aside insertions (e.g., T69T/S/SSS) and deletions (e.g., E/^, T/^)
    #   in order to count the components of each substitution mixture
    #
    # T69T/S/SSS refers to an amino acid insertion between codons 67 and 70 in the
    #   reverse transcriptase of HIV-1. By convention, it is assigned to codon 69.
    insertion_mask = (position_df['MUTATION'].str.contains('[A-Z][A-Z]+'))
    deletion_mask = (position_df['MUTATION'].str.contains('\^', na=False))

    position_df.loc[insertion_mask, 'AA'] = 'ins'
    position_df.loc[deletion_mask, 'AA'] = 'del'
    # remove the '/^' from deletions
    position_df.loc[deletion_mask, 'MUTATION'] = position_df.loc[deletion_mask]['MUTATION'].str.replace('\/.*', '', regex=True)

    insertions_deletions = position_df['AA'].isin(['ins', 'del'])

    ins_del_df = position_df[insertions_deletions].copy()
    # Remove insertions/deletions
    position_df = position_df[~insertions_deletions].copy()

    # Count the number of components in each substitution mixture
    #   (one forward-slash per component)
    position_df['MIXTURE_COMPONENTS'] = [
        aa.count('/')+1 if '/' in aa else 1 for aa in position_df['MUTATION']]

    position_df = position_df.loc[
        np.repeat(position_df.index.values, position_df['MIXTURE_COMPONENTS'])]

    # ACCESSION	    LINE	MUTATION	POSITION    AA      MIXTURE_COMPONENTS
    # -------------------------------------------------------------------------
    # 09-150972	    7	    V245V/M	    245         V/M             2
    # 09-150972	    7	    V245V/M	    245         V/M             2   <<< Added
    # 09-150972     8 	    D123E	    123         E               1

    # drop the index (no longer meaningful after replication)
    position_df.reset_index(drop=True, inplace=True)

    # Number the components of each mixture
    # Must groupby LINE instead of POSITION since,
    #   by filling mutations forward over time,
    #   E40D and E40D/E may appear on the same accession
    position_df['MIXTURE_POSITION'] = position_df.groupby(
        ['ACCESSION', 'LINE', 'MIXTURE_COMPONENTS']).cumcount()

    # LINE	MUTATION	POSITION    AA    MIXTURE_COMPONENTS    MIXTURE_POSITION
    # --------------------------------------------------------------------------
    # 7	    V245V/M	    245         V/M          2                   0
    # 7	    V245V/M	    245         V/M          2                   1
    # 8 	D123E	    123         E            1                   0

    # Extract the component referenced by each position
    # Extract the component referenced by each position
    for i, row in position_df[position_df['MIXTURE_COMPONENTS'] >= 2].iterrows():

        if row['MIXTURE_POSITION'] == 0:
            position_df.loc[i, 'AA'] = re.search(
                '([A-Z])[/d][A-Z]', row['MUTATION']).group(1).strip()

        else:
            this_aa = re.search(
                '[A-Z]/'*(int(row['MIXTURE_POSITION'])) + '([A-Z])', row['MUTATION'])

            if this_aa:
                position_df.loc[i, 'AA'] = this_aa.group(1).strip()
            else:
                # Debugging
                raise ValueError(i, row['MUTATION'], int(row['MIXTURE_POSITION']))

    # LINE	MUTATION	POSITION    AA    MIXTURE_COMPONENTS    MIXTURE_POSITION
    # --------------------------------------------------------------------------
    # 7	    V245V/M	    245         V  **        2                   0
    # 7	    V245V/M	    245         M  **        2                   1
    # 8 	D123E	    123         E            1                   0

    # Drop the mixture columns (no longer needed)
    position_df.drop(
        columns=['MIXTURE_COMPONENTS', 'MIXTURE_POSITION'], inplace=True)

    # Join back in the insertions and deletions that were removed earlier
    position_df = pd.concat([position_df, ins_del_df], axis=0, sort=False)
    position_df['POSITION'] = position_df['POSITION'].astype('float')

    position_df.sort_values(
        ['PAT_MRN_ID', 'COLLECTED_DATE', 'POSITION', 'LINE'], inplace=True)
    position_df.reset_index(inplace=True, drop=True)

    return position_df


def score_positioned_mutations(position_df, scores):

    # Match Stanford's mutation scores for each drug/ARV with observed mutations
    #   (insertions match on 'ins', deletions match on 'del')
    scores_df = position_df.copy()
    for arv in [i for i in scores.columns if i not in ['RULE', 'POSITION', 'AA']]:
        scores_df = scores_df.merge(scores[['POSITION', 'AA', arv]], 
                                    on=['POSITION', 'AA'], how='left')

    # LABEL MUTATIONS IDENTIFIED AS 'RESISTANCE-ASSOCIATED' BY MONOGRAM
    # -----------------------------------------------------------------
    # added after parent function was completed -- otherwise would have been added differently
    # monogram_position_df = _gather_monogram_reported_mutations(mutations_df, loci)
    # scores_df = scores_df.merge(monogram_position_df, how='left')

    print()

    return scores_df


def calculate_total_resistance(position_df, complex_rules):
    import pandas as pd
    import re
    from warnings import simplefilter

    # Future Warning: Setting an item of incompatible dtype is deprecated
    # This appears to be a bug in pandas 2.1.1
    # See https://github.com/pandas-dev/pandas/issues/55025
    simplefilter(action='ignore', category=FutureWarning)

    # PerformanceWarning: DataFrame is highly fragmented.
    # This new to pandas 2.0(+)
    # See https://stackoverflow.com/questions/68292862
    simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

    non_arv_cols = ['RULE', 'POSITION', 'AA', 'RESISTANCE_TYPE']
    arv_cols = [col for col in complex_rules.columns
                if col not in non_arv_cols]

    # Total Scores (copy to modify)
    simple_scores_df = position_df.copy()

    # Rescore dict
    # {Rule: {ARV: penalty}, ...}
    # {'G118R + E138AKT': {'BIC': 10, 'CAB': 10, 'DTG': 10, 'EVG': 10}, ... }
    arv_rescore_dict = complex_rules[arv_cols].to_dict('records')
    arv_rescore_dict = dict(zip(complex_rules['RULE'], arv_rescore_dict))

    # List to hold single-row dataframes
    test_df_list = []

    # For complex rule ...
    for i, row in complex_rules.iterrows():
        pos = re.findall('([A-Z][\d][\d]?[\d]?)', row['RULE'])
        aas = re.findall('[\d][\d]?[\d]?([A-Z]+)', row['RULE'])

        # Initialize a list (not required; here for clarity)
        accessions_list = list()

        # For each component of rule [i] ...
        for j in range(0, len(pos)):
            # Get all accession IDs where first component of rule [i] found
            if j == 0:
                accessions_list = position_df[
                    position_df['MUTATION'].str.startswith(pos[j]) &
                    position_df['AA'].str.contains(f'[{aas[j]}]', regex=True)
                    ]['ACCESSION'].to_list()

            # Get all accession IDs where 
            #   remaining components [j >0] of rule [i] found
            else:
                # Recursively update the accession_list[ ]
                accessions_list = position_df[
                    position_df['ACCESSION'].isin(accessions_list) &
                    position_df['MUTATION'].str.startswith(pos[j]) &
                    position_df['AA'].str.contains(f'[{aas[j]}]', regex=True)
                    ]['ACCESSION'].to_list()

        # For each test accession to which the rule applies ...
        for test in set(accessions_list):
            # Create a single row dataframe
            #
            # ACCESSION ABC AZT FTC 3TC TDF ... RULE
            # -----------------------------------------------
            # 18-157409 15  0   0   0   0   ... L74IV + M184V
            test_df = pd.DataFrame(
                index=[test],
                data=arv_rescore_dict[row['RULE']]).reset_index()
            test_df.rename(columns={'index': 'ACCESSION'}, inplace=True)
            test_df['RULE'] = row['RULE']
            # Append to the growing list
            test_df_list.append(test_df)

    # Create DataFrame from list of dataframes
    complex_rules_df = pd.concat(test_df_list)
    complex_rules_df.reset_index(inplace=True, drop=True)

    # For each rule observed ...
    for i, row in complex_rules_df.iterrows():
        # Extract a '-' delimitted list of the positions
        #   (exclude any/all amino acids involved in a mutation)
        positions = [re.search('([A-Z][\d]+)', i)
                     .group(1).strip() for i in row['RULE'].split('+')]
        complex_rules_df.loc[i, 'POSITIONS'] = '-'.join(positions)

    # Take the max *complex* penalty at each position
    #
    # ACCESSION	    ABC	    AZT	    FTC	 ...   RULE	            POSITIONS
    # -------------------------------------------------------------------
    # 06-153017	    10.0	10.0	0.0	 ...   Q151M + M184IV	Q151-M184
    # 09-135810	    10.0	10.0	0.0	 ...   Q151M + M184IV	Q151-M184
    complex_rules_df_max = complex_rules_df\
        .drop(columns='RULE')\
        .groupby(['ACCESSION', 'POSITIONS'])\
        .agg('max')\
        .reset_index()

    # Validate at
    # https://hivdb.stanford.edu/hivdb/by-patterns/

    # Take the max *simple* penalty at each position
    # And sum all position-penalties within each test accession
    #
    # PAT_MRN_ID	COLLECTED_DATE	ACCESSION	ABC	AZT	FTC	...
    # ----------------------------------------------------------------------
    # 9415	        2013-06-28	    13-134628	0.0	0.0	0.0	...
    # 33464	        2006-07-21	    06-132370	0.0	0.0	0.0	...
    position_cols = ['PAT_MRN_ID', 'COLLECTED_DATE', 'ACCESSION', 'POSITION']
    simple_scores_df = simple_scores_df[position_cols + arv_cols]\
        .groupby(position_cols)\
        .agg('max')\
        .reset_index()\
        .drop(columns='POSITION')\
        .groupby(position_cols[0:3])\
        .agg('sum')\
        .reset_index()

    # Transform *simple* scores from wide to long
    #
    # PAT_MRN_ID	COLLECTED_DATE	ACCESSION	ARV	    SIMPLE_SCORE
    # --------------------------------------------------------------
    # 9415	        2013-06-28	    13-134628	ABC	    0.0
    # 33464	        2006-07-21	    06-132370	ABC	    0.0
    simple_scores_df_melt = simple_scores_df\
        .groupby(['PAT_MRN_ID', 'COLLECTED_DATE', 'ACCESSION'])\
        .agg('sum')\
        .reset_index()\
        .melt(id_vars=['PAT_MRN_ID', 'COLLECTED_DATE', 'ACCESSION'],
              var_name='ARV',
              value_name='SIMPLE_SCORE')

    # Transform *complex* scores from wide to long
    #
    # ACCESSION	    ARV	    COMPLEX_PENALTY
    # -------------------------------------
    # 02-126831	    ABC	    20.0
    # 02-128088	    ABC	    0.0
    complex_rules_df_melt = complex_rules_df_max\
        .drop(columns='POSITIONS')\
        .groupby('ACCESSION')\
        .agg('sum')\
        .reset_index()\
        .melt(id_vars=['ACCESSION'],
              var_name='ARV',
              value_name='COMPLEX_PENALTY')

    # Join *simple* and *complex* scores
    total_scores_df = simple_scores_df_melt.merge(
        complex_rules_df_melt,
        left_on=['ACCESSION', 'ARV'], 
        right_on=['ACCESSION', 'ARV'], how='outer')

    # Total score = Simple score + Complex score
    total_scores_df.fillna(0, inplace=True)
    total_scores_df['TOTAL_SCORE'] = \
        total_scores_df[['SIMPLE_SCORE', 'COMPLEX_PENALTY']].sum(axis=1,
                                                                 skipna=True)

    # Categorize Stanford predicted level of resistance
    total_scores_df.loc[
        total_scores_df['TOTAL_SCORE'].between(0, 9),
        'RESISTANCE_CATEGORY'] = 'Suceptible'

    total_scores_df.loc[
        total_scores_df['TOTAL_SCORE'].between(10, 14),
        'RESISTANCE_CATEGORY'] = 'Potential low-level'

    total_scores_df.loc[
        total_scores_df['TOTAL_SCORE'].between(15, 29),
        'RESISTANCE_CATEGORY'] = 'Low'

    total_scores_df.loc[
        total_scores_df['TOTAL_SCORE'].between(30, 59),
        'RESISTANCE_CATEGORY'] = 'Intermediate'

    total_scores_df.loc[
        total_scores_df['TOTAL_SCORE'] >= 60,
        'RESISTANCE_CATEGORY'] = 'High'

    return total_scores_df


def parse_phenotypic_reports(path_to_zips, test_type):
    """
    Returns two dataframes;

    (1) Test dataframe contains one row per test accession/order

    (2) EAV (entity-attribute-value) dataframe contains
        one row per ARV, per test accession

        # ARV   CUTOFF	FOLD_CHANGE	...	RESISTANCE_CAT  ...
        # -------------------------------------------------
        # 3TC   3.5         9.74    ...     Resistant   ...
        # ABC   NaN         1.87    ...     Sensitiive  ...
    """
    from src.utils import extract_from_zips, scrape_PDF_to_text
    from ipypb import track
    import pandas as pd
    import numpy as np
    import re
    import warnings

    def _mark_report_as_complete(sample_text):
        import re

        report_complete = True

        phrases = ['common causes of assay failure',
                   'unable to perform testing',
                   'quantity not sufficient',
                   'inadequate specimen volume',
                   'low sample volume',
                   'is not ready to report',
                   'canceled per client',
                   'insufficient HIV-infected cells',
                   'duplicate order',
                   'could not be obtained',
                   'could not be completed',
                   'this sample should not be used',
                   'inappropriately collected',
                   'resubmit a new specimen',
                   'resubmit a new sample',
                   'unsuccessful testing of this sample'
                   ]

        for phrase in phrases:
            if report_complete:
                if re.search(phrase, sample_text, flags=re.IGNORECASE):
                    report_complete = False
            else:
                pass

        return report_complete

    def _extract_order_info_as_dict(sample_text: str) -> dict:
        """
        Extract order info from text of parsed PDF report.
        The data elements always follow the same ordering though
        some may be missing.

        Parameters
        ----------
        sample_text : str
            Text from parsed PDF report

        Returns
        ----------
        order_info_dict : dict
        """
        import re
        import numpy as np

        # Get phrasing used for MRN
        if re.search('Medical Record #', sample_text):
            mrn_prefix = 'Medical Record #'
        else:
            mrn_prefix = 'Patient ID'

        # Get phrasing used for Lab Order ID
        if re.search("ID/Order #", sample_text):
            prov_suffix = 'Reference Lab ID/Order #'
        else:
            prov_suffix = 'Reference Lab ID'

        full_name = re.search(
            'Patient Name[:\s]*([A-Za-z -/.]*?)(?=DOB)', sample_text)
        birth_date = re.search(
            'DOB[:\s]*([A-Za-z0-9-/]*?)[ ]*(?=Patient ID)', sample_text)
        mrn = re.search(
            f'{mrn_prefix}[:\s]*([\d]*?)[ \D]*(?=Gender)', sample_text)

        # occassionally Monogram placed the SSN in place of the PAT_MRN_ID
        ssn = re.search(
            f'{mrn_prefix}[:\s]*(\d\d\d-\d\d-\d\d\d\d)[ \D]*(?=Gender)',
            sample_text)
        if ssn:
            mrn = None

        gender = re.search(
            'Gender[:\s]*([A-Za-z]*?)[ ]*(?=Monogram)', sample_text)
        test_accession = re.search(
            'Accession[:\s#]*([A-Za-z0-9-/_]*?)[ ]*(?=Date)', sample_text)
        # Dates follow the format:  17-MAY-2016 11:20 PT
        collection_date = re.search(
            'Date Collected[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        received_date = re.search(
            'Date Received[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        reported_date = re.search(
            'Date Reported[:\s]*([A-Za-z0-9-/]*?)[ ][0-9][0-9]:', sample_text)
        # Mode in [F, M, W]
        test_mode = re.search(
            'Mode[:\s]*([A-Z,]*?)[ ]*(?=Report)', sample_text)
        report_status = re.search(
            'Report Status[:\s]*([A-Z]*?)[ ]*(?=Referring)', sample_text)
        # Referring Physician
        referring_prov = re.search(
            f'Referring Physician[:\s]*([A-Za-z\s\d,]*?)(?={prov_suffix})',
            sample_text)
        # keep only first two names (do not keep address, if present)
        if referring_prov:
            referring_prov = re.search(
                '([A-Za-z]*[ ][A-Za-z]*)', referring_prov.group(1))
        # Order ID should not contain letters
        # ... in the early days the hospital floor was substituted
        order_id = re.search(
            f'{prov_suffix}[:\s]*([0-9]*?)[ _A-Z-:]', sample_text)

        val_list = [full_name, birth_date, mrn, ssn, gender, test_accession,
                    collection_date, received_date, reported_date, test_mode,
                    report_status, referring_prov, order_id]

        row_values = [v.group(1).strip()
                      .title() if v else np.NaN for v in val_list]

        col_names = ['FULL_NAME', 'BIRTH_DATE', 'PAT_MRN_ID', 'SSN', 'GENDER',
                     'ACCESSION', 'COLLECTED_DATE', 'RECEIVED_DATE',
                     'REPORTED_DATE', 'MODE', 'REPORT_STATUS',
                     'REFERRING_PROV', 'ORDER_ID']

        order_info_dict = dict(zip(col_names, row_values))

        return order_info_dict

    def _extract_fold_change_as_dict(sample_text: str, test_type: str) -> dict:
        """
        Extract fold change info from text of parsed PDF report.
        The data elements always follow the same ordering though
        some may be missing.

        Parameters
        ----------
        sample_text : str
            Text from parsed PDF report

        Returns
        ----------
        arv_fold_change_dict : dict
            Keys are ARVs, Values are a dictionary
        """
        from src.utils import arv_master_dict
        import re

        # Empty ARV Fold Change dict
        arv_fold_change_dict = {}

        ##################
        # Phenosense-Entry
        ##################
        if test_type.upper() == 'PHENOSENSE-ENTRY':
            # Enfuvirtide Fuzeon0.019434 0.51    ENF
            ic50_fold_change =re.search(
                '(?<=Enfuvirtide)[ ]*Fuzeon([\d.]+)[ ]+([\d.]+)[ ]*(?=ENF)',
                sample_text)

            if not ic50_fold_change:
                arv_fold_change_dict['ENF'] = {
                    'IC50': None,
                    'FOLD_CHANGE': None,
                    'BIOLOGICAL_CUTOFF': None
                }

            # The upper bound for ENV sensitivity is a fold change of 6.48
            # The lower bound (hyper-sensitivity) is 0.42
            if ic50_fold_change:
                arv_fold_change_dict['ENF'] = {
                    'IC50': float(ic50_fold_change.group(1).strip()),
                    'FOLD_CHANGE': float(ic50_fold_change.group(2).strip()),
                    'BIOLOGICAL_CUTOFF': 6.48
                }

        ######################
        # All other test types
        ######################
        else:
            for art_class, art_list in arv_master_dict.items():
                # {'NRTI': [('Ziagen', 'Abacavir', 'ABC', '1998-12-17'), ...],
                #  'NNRTI': [('Pifeltro', 'Doravirine', 'DOR', '2018-08-30')
                #   ...
                for attr in art_list:

                    # Switch for Phenosense-GT tests (different ending)
                    if test_type.upper() == 'PHENOSENSE-GT':
                        suffix = '[ YN]+?'
                    else:
                        suffix = f'[ ]*?(?={attr[2]})'

                    # Capture groups
                    g1 = '([\d.\s-]+)'
                    g2 = '([\d.>MAX]+?)'

                    # Unboosted ARVs
                    unboosted = re.search(
                        f'{attr[1]}[ ]+{attr[0]}[ ]*\({g1}\)[ ]*?{g2}{suffix}',
                        sample_text)

                    if unboosted:
                        fold_change = unboosted.group(2)

                        # (lower, upper)
                        if '-' in unboosted.group(1):
                            lower_cutoff, upper_cutoff = unboosted\
                                .group(1).split('-')

                            arv_fold_change_dict[attr[2]] = {
                                'FOLD_CHANGE': fold_change.strip(),
                                'LOWER_CUTOFF': lower_cutoff.strip(),
                                'UPPER_CUTOFF': upper_cutoff.strip()
                            }

                        # (single cutoff)
                        else:
                            arv_fold_change_dict[attr[2]] = {
                                'FOLD_CHANGE': fold_change.strip(),
                                'BIOLOGICAL_CUTOFF': unboosted.group(1).strip()
                                }

                    # Boosted ARVs include '/r' after brand name
                    boosted = re.search(
                        f'{attr[0]}[ ]*\/[ ]r[ ]*\({g1}\)[ ]*?{g2}{suffix}',
                        sample_text)

                    if boosted:
                        fold_change = boosted.group(2)

                        # (lower, upper)
                        if '-' in boosted.group(1):
                            lower_cutoff, upper_cutoff = boosted\
                                .group(1).split('-')

                            arv_fold_change_dict[f'{attr[2]}_r'] = {
                                'FOLD_CHANGE': fold_change.strip(),
                                'LOWER_CUTOFF': lower_cutoff.strip(),
                                'UPPER_CUTOFF': upper_cutoff.strip()
                                }

                        # (single cutoff)
                        else: 
                            arv_fold_change_dict[f'{attr[2]}_r'] = {
                                'FOLD_CHANGE': fold_change.strip(),
                                'BIOLOGICAL_CUTOFF': boosted.group(1).strip()
                                }

        # Add resistance category to dict
        for arv, attr in arv_fold_change_dict.items():
            fold_change = arv_fold_change_dict[arv]['FOLD_CHANGE']

            if '>' in attr['FOLD_CHANGE']:
                attr['RESISTANCE_CAT'] = 'Resistant'

            # Biological cutoffs are used for specific antiretrovirals
            elif 'BIOLOGICAL_CUTOFF' in attr.keys():
                if float(fold_change) > float(attr['BIOLOGICAL_CUTOFF']):
                    attr['RESISTANCE_CAT'] = 'Resistant'

                elif float(fold_change) <= float(attr['BIOLOGICAL_CUTOFF']):
                    attr['RESISTANCE_CAT'] = 'Sensitive'

            # All others use clinical cutoffs
            else:
                if float(fold_change) > float(attr['UPPER_CUTOFF']):
                    attr['RESISTANCE_CAT'] = 'Resistant'

                elif float(fold_change) <= float(attr['LOWER_CUTOFF']):
                    attr['RESISTANCE_CAT'] = 'Sensitive'

                else:
                    attr['RESISTANCE_CAT'] = 'Partially Sensitive'

            arv_fold_change_dict[arv] = attr

        return arv_fold_change_dict

    def _extract_ic50_as_dict(sample_text: str) -> dict:
        import re

        drugs = re.search(
            '(?<=Patient-specific)[ ]*Results[ ]*Drugs[ ]*([A-Za-z\d\s/]+?)(?=IC50)', sample_text)
        # all digits and ">MAX" until start of Fold-change results
        ic50 = re.search(
            '(?<=IC50)[\D]*?([\d\s.>MAX]*)(?=F)', sample_text)

        ic50_dict = dict(zip(drugs.group(1).strip().split(' '),
                             ic50.group(1).strip().split(' ')))

        return ic50_dict

    def _extract_replication_capacity_tuple(sample_text: str):
        # Phenosense Integrase added the "Integrase" word to the text below
        # (?=Replication)
        # Nevertheless, the search phrase is specific enough that
        #   it does not yield any false-positives
        rep_capacity_and_bounds = re.search(
            '(?<=Capacity)[ ]*=[ ]*([\d.]+)%\(Range[ ]*([\d.]+)%-([\d.]+)%\)',
            sample_text)

        if rep_capacity_and_bounds:
            rep_capacity = float(rep_capacity_and_bounds.group(1).strip())/100
            lower_bound = float(rep_capacity_and_bounds.group(2).strip())/100
            upper_bound = float(rep_capacity_and_bounds.group(3).strip())/100

            return (rep_capacity, lower_bound, upper_bound)

        else:
            return (None, None, None)

    def _reorder_dataframe_columns(df) -> pd.DataFrame:
        # get list
        cols = df.columns.to_list()

        # order info
        order_info = ['DOC_NAME', 'TEST_TYPE', 'REPORT_COMPLETE', 'FULL_NAME',
                      'PAT_MRN_ID', 'SSN', 'BIRTH_DATE', 'GENDER', 'ACCESSION',
                      'ORDER_ID', 'REFERRING_PROV', 'COLLECTED_DATE',
                      'RECEIVED_DATE', 'REPORTED_DATE', 'REPORT_STATUS',
                      'MODE', 'REPLICATION_CAPACITY', 'REPLICATION_LOWER',
                      'REPLICATION_UPPER']

        order_info = [i for i in order_info if i in cols]

        # ARV (resistance results) ordering
        nrti = ['3TC', 'ABC', 'd4T', 'ddC', 'ddI', 'FTC', 'TFV', 'TAF', 'ZDV']
        nnrti = ['DOR', 'DLV', 'EFV', 'ETR', 'NVP', 'RPV']
        insti = ['BIC', 'CAB', 'DTG', 'EVG', 'RAL']
        pi = ['AMP', 'AMPr', 'ATV', 'ATVr', 'DRV', 'DRVr', 'IDV', 'IDVr',
              'LPV', 'LPVr', 'NFV', 'NFVr', 'RTV', 'RTVr', 'SQV', 'SQVr',
              'TPV', 'TPVr']
        fusion = ['ENF']
        # trofile

        nrti_cols = [i for i in nrti if i in cols]
        nnrti_cols = [i for i in nnrti if i in cols]
        insti_cols = [i for i in insti if i in cols]
        pi_cols = [i for i in pi if i in cols]
        fusion_cols = [i for i in fusion if i in cols]

        # create re-ordered list of columns
        cols_reordered = order_info + nrti_cols + nnrti_cols + pi_cols + insti_cols + fusion_cols
        not_listed_error = [i for i in cols if i not in cols_reordered]

        # apply it
        df = df[cols_reordered + not_listed_error]

        return df

    # Future Warning: Setting an item of incompatible dtype is deprecated
    # This appears to be a bug in pandas 2.1.1
    # See https://github.com/pandas-dev/pandas/issues/55025
    warnings.simplefilter(action='ignore', category=FutureWarning)

    allowable_test_types = ['PHENOSENSE', 'PHENOSENSE-GT',
                            'PHENOSENSE-INTEGRASE', 'PHENOSENSE-ENTRY']
    if test_type.upper() not in allowable_test_types:
        raise ValueError(f'Invalid test type ({allowable_test_types})')

    # Extract PDF reports from Zip files
    report_paths = extract_from_zips(path_to_zips)

    # Empty TEST dataframe (to fill)
    # Many single-row insertions
    df = pd.DataFrame()

    # Empty list (to fill with dataframes)
    # With multi-row dataframes it is more efficient to concatenate a list
    #   due to the overhead of recreating the dataframe's index
    eav_df_list = list()

    # Loop over PDF's
    for i in track(range(0, len(report_paths)), label='Records'):

        # Parse PDF to text
        sample_text = scrape_PDF_to_text(report_paths[i])

        # Extract order info
        order_info_dict = _extract_order_info_as_dict(sample_text=sample_text)

        # Insert order info into dataframe
        df.loc[i, 'TEST_TYPE'] = test_type.upper()
        df.loc[i, 'DOC_NAME'] = report_paths[i].name

        for attribute, value in order_info_dict.items():
            df.loc[i, attribute] = value

        # If report is not marked "incomplete"
        #   gather oberserved mutations
        report_complete = _mark_report_as_complete(sample_text=sample_text)
        df.loc[i, 'REPORT_COMPLETE'] = report_complete

        if report_complete:
            fold_change_dict = _extract_fold_change_as_dict(
                sample_text=sample_text, test_type=test_type)

            # Entity-attribute-value (EAV) format
            # One row per drug
            #
            # ARV   CUTOFF	FOLD_CHANGE	LOWER_CUTOFF	RESISTANCE_CAT	UPPER_CUTOFF
            # ----------------------------------------------------------------------
            # 3TC   3.5         9.74        NaN             Resistant	    NaN
            # ABC   NaN         1.87        4.5             Sensitiive	    6.5
            fold_change_eav_df = pd.melt(
                pd.DataFrame(fold_change_dict).reset_index(),
                id_vars='index', var_name='ARV')\
                .pivot(index='ARV', columns='index', values='value')

            #################################################
            # IC50
            #
            # Was extacted in step above for Phenosense-Entry
            #################################################
            if not test_type.upper() == 'PHENOSENSE-ENTRY':
                try:
                    ic50_dict = _extract_ic50_as_dict(sample_text=sample_text)

                    # Insert IC50 into EAV dataframe
                    for arv, ic50 in ic50_dict.items():
                        fold_change_eav_df.loc[arv, 'IC50'] = ic50
                        # IC50 value applies to the unboosted drug
                        # Only lower/upper cutoff values change with boosting
                        # So apply the value to both boosteda and unboosted
                        fold_change_eav_df.loc[f'{arv}_r', 'IC50'] = ic50

                    # Remove rows where IC50 added but no fold-change reported
                    # For example, when only boosted DRV was reported drop
                    #   the row for unboosted DRV added by the above loop
                    fold_change_eav_df = fold_change_eav_df[
                        fold_change_eav_df['RESISTANCE_CAT'].notnull()]

                except ValueError:
                    pass

            # Move 'ARV' field out of index
            #   and append to list of EAV dataframes
            fold_change_eav_df['DOC_NAME'] = report_paths[i].name
            fold_change_eav_df.reset_index(inplace=True)
            eav_df_list.append(fold_change_eav_df)

            # Insert resistance determination into TEST dataframe
            for arv, fold_change_attributes in fold_change_dict.items():
                df.loc[i, arv] = fold_change_attributes['RESISTANCE_CAT']

            ###################################
            # Replication Capacity
            #
            # Not reported for Phenosense-Entry
            ###################################
            if not test_type.upper() == 'PHENOSENSE-ENTRY':
                # if not re.search('Replication capacity cannot be reported',
                #                 sample_text):
                try:
                    rep_capacity = _extract_replication_capacity_tuple(
                        sample_text=sample_text)

                    # Insert replication capacity into TEST dataframe
                    df.loc[i, 'REPLICATION_CAPACITY'] = rep_capacity[0]
                    df.loc[i, 'REPLICATION_LOWER'] = rep_capacity[1]
                    df.loc[i, 'REPLICATION_UPPER'] = rep_capacity[2]

                except ValueError:
                    pass

        # Reorder columns (aesthetic)
        df = _reorder_dataframe_columns(df=df)

    # Combine EAV dataframes
    eav_df = pd.concat(eav_df_list)
    eav_df.reset_index(inplace=True, drop=True)

    # Re-sort TEST dataframe by patient
    df.sort_values(['PAT_MRN_ID', 'ACCESSION'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Set '' (artefact of regular expression) to missing
    for col in [i for i in df.columns if df[i].dtype == 'O']:
        df.loc[df[col] == '', col] = np.NaN

    # Dates
    for date_col in ['BIRTH_DATE', 'COLLECTED_DATE',
                     'RECEIVED_DATE', 'REPORTED_DATE']:
        df[date_col] = pd.to_datetime(df[date_col], format='mixed')

    # MRN can be up to 8 characters (if not starting with '0')
    #   ... but if < 7 it is zero-padded to 7 characters
    # Will need to perform manual chart review for missing MRNs
    mrn_length = df['PAT_MRN_ID'].str.len()
    leading_zero = df['PAT_MRN_ID'].str.startswith('0')

    df.loc[(mrn_length > 8) & (leading_zero),
           'MRN_OUTDATED'] = df['PAT_MRN_ID'].str.slice(1, )
    df.loc[(mrn_length > 8), 'PAT_MRN_ID'] = np.NaN

    df.loc[(mrn_length == 8) & (leading_zero),
           'PAT_MRN_ID'] = df['PAT_MRN_ID'].str.slice(1, )
    df.loc[(mrn_length == 8), 'MRN_OUTDATED'] = 1

    df.loc[mrn_length < 7, 'PAT_MRN_ID'] = df['PAT_MRN_ID'].str.zfill(7)

    # Manual fix (no info on PDF)
    # df.loc[df['DOC_NAME'] == '96063822_18-184109-1GN-0_F.PDF',
    #       'ACCESSION'] = '18-184109'

    print(f"{i+1} {test_type} records parsed")

    return df, eav_df
