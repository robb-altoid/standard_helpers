import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

# package the function to generate hs spectra data pivot tables
def convert_to_spectral(input_df: pd.DataFrame, 
                        pov_name_column: str, 
                        pivot_column: str, 
                        counting_column: str = 'transaction_id',
) -> pd.DataFrame:
    """ 
    This function outputs a pandas dataframe (df) with Harmonized Shipping Code (HS) spectral data from a df with 
    transaction data. 

    An HS spectrum is a count of the number of shipments in each HS category that were sent or received by a company in 
    the list of transactions. The spectra can be generated from either shipping or receiving data. The input df should 
    have transaction data from either default.transaction_table21 (d.tt21) or atlas_kg_db_21.facility_sends_to_edge 
    (akd21.fste). 

    Args:
        input_df:   An extract from either d.tt or akd2.fste. The input data must include only the following columns:
                        -   pov_name_column: the column with the name or address of either the receiver or sender that 
                            will be the grouping index
                        -   pivot_column: the column whose values will become the columns in the pivot table (generally 
                            either hs2 or hs6)
                        -   counting_column: a column that's guaranteed to have data in it so that it can be counted, 
                            such as "transaction_id" in either akd2.fste or d.tt

        pov_name_column:    The name of the column that will become the rows in the pivot_df

        pivot_column:       The name of the column whose values will become the row names in the pivot_df

        counting_column:    The column that will be counted to generate the number of transactions in each HS category

    Returns:
        pivot_df:   The spectral data for each company with HS categories as columns and company names as the row 
                    indicies
    """
   
    pivot_df = (input_df[[pov_name_column, pivot_column, counting_column]]
                .groupby([pov_name_column,pivot_column])
                .count()
                .reset_index())
    pivot_df = pivot_df.pivot_table(index=pov_name_column, columns=pivot_column, values=counting_column, fill_value=0) 
    
    return pivot_df



def hs_spectra_comparator(know_actors_transactions_df: pd.DataFrame, 
                          candidate_pop_transactions_df: pd.DataFrame, 
                          known_actors_spectra_df: pd.DataFrame, 
                          candidate_pop_spectra_df: pd.DataFrame,
                          known_grouping_name: str,
                          candidate_pop_grouping_name: str
                          ) -> pd.DataFrame:
    """
    This function performs a pairwise cosine similarity comparison and spectral identify of the vectors of HS spectra
    from a target population to a candidate population. The function then adds additional statistics to output a 
    pandas dataframe that is directly relevant for an analyst trying to determine if any company in a candidate 
    population is sending or receiving similar HS goods. The function can compare any level of HS code (2,4,6), the 
    desired comparison value has to be part of the input dataframes. The function returns a pandas dataframe with the 
    result of each comparison and the additional statistics.

    Args:
        know_actors_transactions_df: a pandas dataframe that contains all of the known actors' transactions from either
                                      akd21.fste or d.tt. The df must contain a column of hs codes for comparsion (this
                                      should be a substring of the hs_code column in the db to ensure that they are all 
                                      the same lenght - this isn't true yet because I haven't integrated the "convert_to
                                      _spectral" function into this function).
            
        candidate_pop_transactions_df:  a pandas dataframe that contains all of the candidate population's transactions
                                        from either akd21.fste or d.tt. The df must contain a column of hs codes for  
                                        comparsion (this should be a substring of the hs_code column in the db to ensure
                                        that they are all the same lenght - this isn't true yet because I haven't
                                        integrated the "convert_to_spectral" function into this function).

        known_actors_spectra_df:    a pandas dataframe of the hs spectra of the known population (of known bad actors for 
                                    example).

        candidate_pop_spectra_df:   a pandas dataframe of the hs spectra of the target population (the population where
                                    you looking to see if any of the members have signatures closely related to the 
                                    known actors' signatures).

        known_grouping_name:    name of the column in know_actors_transactions_df that is either the company name or 
                                created composite (can be either sender or receiver but both this and 
                                candidate_pop_grouping_name should have the same transaction perspective).

        candidate_pop_grouping_name:    name of the column in candidate_pop_transactions_df that is either the company
                                        name or created composite (can be either sender or receiver but both this and 
                                        known_grouping_name should have the same transaction perspective).

    Returns:
        kn_to_unk_comparision_df:   the pandas dataframe with the pairwise comparisons of each known to each candidate.
    """

    # will use a transfer_df to hold the two spectra that will be compared. need a set of the hs2 columns from both spectra
    # to create the transfer_df each time 
    kn_cols_lst = known_actors_spectra_df.columns.tolist()
    unk_cols_lst = candidate_pop_spectra_df.columns.tolist()
    kn_cols_lst.extend(unk_cols_lst)
    combined_cols_lst = list(set(kn_cols_lst))
    combined_cols_lst.sort()

    # this is the df for holding the comparison results
    kn_to_unk_comparision_df = pd.DataFrame(columns=['left_facility','right_facility','cos_sim'])

    # loop through each row of the spectra of the known t0 companies and compare it to each row
    # of the unknown t0 companies
    i = 0
    for ind,row in known_actors_spectra_df.iterrows():
        for ind2,row2 in candidate_pop_spectra_df.iterrows():
            try:
                del transfer_df
            except NameError:
                pass
            transfer_df = pd.DataFrame(columns=combined_cols_lst)
            transfer_df = transfer_df.append(row)  
            transfer_df = transfer_df.append(row2)
            transfer_df = transfer_df.fillna(0)
            # cos_sim returns a matrix. the [0,1] enables gathering the 2nd value in the first array. 
            # it's the same thing as doing "matrix[0][1]"
            score = cosine_similarity(transfer_df)[0,1]
            transfer_dict = {'left_facility':ind,'right_facility':ind2, 'cos_sim':score}
            kn_to_unk_comparision_df = kn_to_unk_comparision_df.append(transfer_dict, ignore_index=True)
   
    
    # add the count of transactions
    ## create dictionary of the sum of transactions for each company from the original spectra dfs
    unk_to_rur1_sum_trans_dict = candidate_pop_spectra_df.sum(axis=1).to_dict()
    kn_to_rur1_sum_trans_dict = known_actors_spectra_df.sum(axis=1).to_dict()

    ## add the sum of transaction counts to kn_to_unk_comparision_df
    kn_to_unk_comparision_df['count_left_trans'] = kn_to_unk_comparision_df['left_facility'].map(kn_to_rur1_sum_trans_dict)
    kn_to_unk_comparision_df['count_right_trans'] = kn_to_unk_comparision_df['right_facility'].map(unk_to_rur1_sum_trans_dict)


    #  add the earliest and latest transaction dates to kn_to_unk_comparision_df
    ## remove any rows where transaction_date is NAN in all the transactions in known_actors_spectra_df 
    know_actors_for_minmax_dates_df = know_actors_transactions_df[[known_grouping_name,'transaction_date']].copy()
    know_actors_for_minmax_dates_df = know_actors_for_minmax_dates_df[know_actors_for_minmax_dates_df['transaction_date'].notna()]
    know_actors_for_minmax_dates_df['transaction_date'] = pd.to_datetime(know_actors_for_minmax_dates_df['transaction_date'])
    know_actors_for_minmax_dates_df['transaction_date'] = know_actors_for_minmax_dates_df['transaction_date'].dt.date

    ## remove any rows where transaction_date is NAN in all the transactions in known_actors_spectra_df 
    candidate_pop_for_minmax_dates_df = candidate_pop_transactions_df[[candidate_pop_grouping_name,'transaction_date']].copy()
    candidate_pop_for_minmax_dates_df = candidate_pop_for_minmax_dates_df[candidate_pop_for_minmax_dates_df['transaction_date'].notna()]
    candidate_pop_for_minmax_dates_df['transaction_date'] = pd.to_datetime(candidate_pop_for_minmax_dates_df['transaction_date'])
    candidate_pop_for_minmax_dates_df['transaction_date'] = candidate_pop_for_minmax_dates_df['transaction_date'].dt.date

    ## create dicts of every company and their min/max transaction dates
    kn_min_trans_date_dict = {}
    kn_max_trans_date_dict = {}
    for name in know_actors_for_minmax_dates_df[known_grouping_name].sort_values().unique().tolist():
        kn_min_trans_date_dict[name] = know_actors_for_minmax_dates_df[know_actors_for_minmax_dates_df[known_grouping_name]==name]['transaction_date'].min()
        kn_max_trans_date_dict[name] = know_actors_for_minmax_dates_df[know_actors_for_minmax_dates_df[known_grouping_name]==name]['transaction_date'].max()

    candidate_min_trans_date_dict = {}
    candidate_max_trans_date_dict = {}
    for name in candidate_pop_for_minmax_dates_df[candidate_pop_grouping_name].sort_values().unique().tolist():
        candidate_min_trans_date_dict[name] = candidate_pop_for_minmax_dates_df[candidate_pop_for_minmax_dates_df[candidate_pop_grouping_name]==name]['transaction_date'].min()
        candidate_max_trans_date_dict[name] = candidate_pop_for_minmax_dates_df[candidate_pop_for_minmax_dates_df[candidate_pop_grouping_name]==name]['transaction_date'].max()
        
    ## add the min and max transaction dates for known and unknown t0 facilities
    kn_to_unk_comparision_df['left_min_trans_date'] = kn_to_unk_comparision_df['left_facility'].map(kn_min_trans_date_dict)
    kn_to_unk_comparision_df['left_max_trans_date'] = kn_to_unk_comparision_df['left_facility'].map(kn_max_trans_date_dict)
    kn_to_unk_comparision_df['right_min_trans_date'] = kn_to_unk_comparision_df['right_facility'].map(candidate_min_trans_date_dict)
    kn_to_unk_comparision_df['right_max_trans_date'] = kn_to_unk_comparision_df['right_facility'].map(candidate_max_trans_date_dict)


    # Add counts of HS categories in common between the left and right entities
    for index,row in kn_to_unk_comparision_df.iterrows():

        # get the hs spectra of the two entities receiving or shipping goods and store them as a dictionary
        left_loc_items_dict = known_actors_spectra_df.loc[row['left_facility']].to_dict()
        right_loc_items_dict = candidate_pop_spectra_df.loc[row['right_facility']].to_dict()    
        
        # get the keys from both dictionaries (the keys are the hs2 categories)
        left_keys = [x[0] for x in left_loc_items_dict.items() if x[1] != 0]
        right_keys = [x[0] for x in right_loc_items_dict.items() if x[1] != 0]
        
        # add count of number of keys for each facility to the df
        kn_to_unk_comparision_df.loc[index,'left_count_hs_cats'] = len(left_keys)
        kn_to_unk_comparision_df.loc[index,'right_count_hs_cats'] = len(right_keys)
        
        # count the number of unique hs2 categories in the two lists
        keys_in_either_lst = left_keys.copy()
        keys_in_either_lst.extend(right_keys)
        count_total_keys = len(set(keys_in_either_lst))
        
        # count the number of hs2 categories in common between the two lists
        count_common_key = len(set(left_keys).intersection(set(right_keys)))
        
        # add proportion of keys in common between the two
        kn_to_unk_comparision_df.loc[index,'prop_hs_counts_in_common'] = count_common_key/count_total_keys

    # change the order of columns to make it easier to interpret the data
    kn_to_unk_comparision_df = kn_to_unk_comparision_df[[
    'left_facility',
    'right_facility',
    'cos_sim',
    'prop_hs_counts_in_common',
    'left_count_hs_cats',
    'right_count_hs_cats',
    'count_left_trans',
    'count_right_trans',
    'left_min_trans_date',
    'left_max_trans_date',
    'right_min_trans_date',
    'right_max_trans_date'
    ]]

    kn_to_unk_comparision_df = kn_to_unk_comparision_df.sort_values('cos_sim', ascending=False)
    
    return kn_to_unk_comparision_df


def comparator_func_train(know_actors_transactions_df: pd.DataFrame, 
                          candidate_pop_transactions_df: pd.DataFrame, 
                          known_grouping_name: str,
                          candidate_pop_grouping_name: str,
                          known_hs_column_for_pivotting: str,
                          candidate_hs_column_for_pivotting: str
                         ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    This function stitches together all the other functions so you can start with two dfs (one each for the transactions 
    of the known actors and candidate population) and get the spectra dataframes plus the output of the 
    hs_spectra_comparator
    
    inputs:
        know_actors_transactions_df:    pd.DataFrame - a pandas dataframe that contains all of the known actors'  
                                        transactions from either akd21.fste or d.tt. The df must contain a column of hs 
                                        codes for comparsion (this should be a substring of the hs_code column in the db
                                        to ensure that they are all the same lenght - this isn't true yet because I 
                                        haven't integrated the "convert_to _spectral" function into this function).
            
        candidate_pop_transactions_df:  pd.DataFrame - a pandas dataframe that contains all of the candidate   
                                        population's transactions from either akd21.fste or d.tt. The df must contain a
                                        column of hs codes for comparsion (this should be a substring of the hs_code 
                                        column in the db to ensure that they are all the same lenght - this isn't true 
                                        yet because I haven't integrated the "convert_to_spectral" function into this 
                                        function).

        known_grouping_name:    str - name of the column in know_actors_transactions_df that is either the company name
                                or created composite (can be either sender or receiver but both this and 
                                candidate_pop_grouping_name should have the same transaction perspective).

        candidate_pop_grouping_name:    str - name of the column in candidate_pop_transactions_df that is either the 
                                        company name or created composite (can be either sender or receiver but both 
                                        this and known_grouping_name should have the same transaction perspective).

        known_hs_column_for_pivotting:  str = the name of the column in the know_actors_transactions_df that has the hs2
                                        (or hs4 or hs6) column that will become the column names in the spectra_df.

        candidate_hs_column_for_pivotting:  the name of the column in the know_actors_transactions_df that has the hs2
                                            (or hs4 or hs6) column that will become the column names in the spectra_df.


    outputs:
        known_actors_spectra_df:    pd.DataFrame - a pandas dataframe of the hs spectra of the known population (of 
                                    known bad actors for example).

        candidate_pop_spectra_df:   pd.DataFrame - a pandas dataframe of the hs spectra of the target population (the 
                                    population where you looking to see if any of the members have signatures closely 
                                    related to the known actors' signatures).

        kn_to_unk_comparision_df:   pd.DataFrame - the pandas dataframe with the pairwise comparisons of each known to 
                                    each candidate.
    """

    # convert the dfs with the transactions for the known and candidate pops into spectra dfs
    know_actors_spectra_df = convert_to_spectral(   know_actors_transactions_df, 
                                                    known_grouping_name, 
                                                    known_hs_column_for_pivotting)

    candidate_pop_spectra_df = convert_to_spectral( candidate_pop_transactions_df, 
                                                    candidate_pop_grouping_name, 
                                                    candidate_hs_column_for_pivotting)

    known_to_aca_comparison_df = hs_spectra_comparator(know_actors_transactions_df,
                                                       candidate_pop_transactions_df,
                                                       know_actors_spectra_df,
                                                       candidate_pop_spectra_df,
                                                       known_grouping_name,
                                                       candidate_pop_grouping_name                              
                                                      )

    return (know_actors_spectra_df, candidate_pop_spectra_df, known_to_aca_comparison_df)