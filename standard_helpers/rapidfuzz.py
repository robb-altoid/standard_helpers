
from fuzzywuzzy import fuzz
import pandas as pd

def fuzzy_name_matching(input_df: pd.DataFrame, company_name_col: str, company_canon_id_col: str) -> pd.DataFrame:
    """
    Takes a dataframe containing a list of company names and returns a dataframe with the pairwise comparison of the 
    names (including company_canon_id for each) and the fuzzywuzzy fuzz.ratio comparison score (fuzz_score). For 
    example, the input dataframe could be like this:

    company_name	                    company_canon_id
    3D IMPORT EXPORT S DE RL DE CV	    d3630d3f-932c-5153-9481-82c0e018cb33
    4e global sapi de cv	            2611f46-a729-5311-acca-e501a456d413
    a b electricals	                    8f2af34e-4b6a-5161-9cf7-239544391d3f

    And the output dataframe would be like this:
    
    fuzz_score	importer_company_name_x	            importer_company_name_y	    importer_company_canon_id_x	            importer_company_canon_id_y
    29	        4e global sapi de cv	            a b electricals	            b2611f46-a729-5311-acca-e501a456d413	8f2af34e-4b6a-5161-9cf7-239544391d3f
    16	        3D IMPORT EXPORT S DE RL DE CV	    4e global sapi de cv	    d3630d3f-932c-5153-9481-82c0e018cb33	b2611f46-a729-5311-acca-e501a456d413
    9	        3D IMPORT EXPORT S DE RL DE CV	    a b electricals	            d3630d3f-932c-5153-9481-82c0e018cb33	8f2af34e-4b6a-5161-9cf7-239544391d3f

    The input dataframe can have any number of columns but it must contain those specified in the Args section. 

    Args
    input_df: pandas.DataFrame

    company_name_col: str
        The name of the column in the input_df that contains the company name that will be used in the fuzzywuzzy name
        matching algorithm.

    company_canon_id_col: str
        The name of the column in the input_df that contains the company's corresponding company_canon_id.

    Returns
    cross_names_df: pandas.DataFrame
        The listing of each pairwise comparison and its fuzzywuzzy.fuzz.ratio score. The dataframe is sorted from 
        highest to lowest fuzz.ratio score, then alphabetically for company_name_x and then company_name_y.
        IMPORTANT_NOTE - the pairwise comparison doesn't contain the mirror images (it will only return a/b and not b/a)
        so you will need to search for company names in both the company_name_x and company_name_y (or company_canon_id 
        in company_canon_id_x and company_canon_id_y).

    
    """
    
    # drop all other columns from the input_df
    input_df = input_df[[company_name_col,company_canon_id_col]]

    # create a cross join (cartesian product) so that every name is compared against every other name
    cross_names_df = input_df.merge(input_df, how='cross')

    # create variables for the new column names (_x and _y were addded to the column name during the merge)
    company_name_col_x = company_name_col + "_x"
    company_name_col_y = company_name_col + "_y"
    company_canon_id_col_x = company_canon_id_col + "_x"
    company_canon_id_col_y = company_canon_id_col + "_y"

    # remove those pairings of self to self 
    cross_names_df = cross_names_df[cross_names_df[company_canon_id_col_x]!=cross_names_df[company_canon_id_col_y]]

    # remove the cross pairings that are the mirror images (keep a/b but drop b/a)
    cross_names_df['check_string'] = cross_names_df.apply(lambda row: ''.join(sorted([row[company_name_col_x], row[company_name_col_y]])), axis=1)
    cross_names_df.drop_duplicates('check_string', inplace=True)
    cross_names_df.drop('check_string', inplace=True, axis=1)


    # apply the fuzz.ratio function to calculate name similarity
    cross_names_df['fuzz_score'] = cross_names_df.apply(lambda x: fuzz.ratio(x[company_name_col_x],x[company_name_col_y]), axis=1)


    # reorder columsn for easier review
    cross_names_df = cross_names_df[['fuzz_score',
                                    company_name_col_x,
                                    company_name_col_y,
                                    company_canon_id_col_x,
                                    company_canon_id_col_y]]

    # sort so highest matches at top of df
    cross_names_df = cross_names_df.sort_values('fuzz_score', ascending=False)

    return cross_names_df