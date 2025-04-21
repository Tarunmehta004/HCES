# Defining class to calculate item wise average expenditure at the HH level

## Iporting libraries
import pandas as pd
import numpy as np
import os
from IPython.display import display
from typing import Literal

class hh_decile_exp:
    """
    Calculates item-wise average expenditure for each decile.
    """

    #Initialization
    def __init__ (self, current_level_path: str, level_15_path: str, decile_path: str, ic_combined: list, hh_size_str: Literal['F', 'C', 'D'], ic_7days = [], ic_30days = [], ic_365days = [],cons_column = 'cons_total_value'):
        """
        Args:
            current_level_path (str): Path of the .dta file containing data of a particular level.
            level_15_path (str): Path of the .dta file fr level 15 (containing questionnaire wise hh_size).
            decile_path (str): Path of the .pkl file containing decile mapping for HHs. 
            ic_combined (list): Combined list of all item codes in a level.
            hh_size_str (str): A string indicating the original hh_size questionnaire.
            ic_7days (list): List of item codes with recorded expenditure for 7 days.
            ic_30days (list): List of item codes with recorded expenditure for 30 days.
            ic_365days (list): List of item codes with recorded expenditure for 365 days. 
            cons_column (str): Name of the column to aggregate. Acceptable values: cons_home_qty, cons_home_value, cons_total_qty,\
                cons_total_value. Default: cons_total_value. 
        """
        
        #Loading current level file
        self.level_df = pd.read_stata(os.path.normpath(current_level_path))
        if 'item_code' in self.level_df.columns:                            # Checks if the level file has item_code column
            self.level_df ['item_code'] = self.level_df['item_code'].\
                map(lambda x: str(x).zfill(3))                              #Convert itemcode to 3 digit strings
            print("\n"*2 + "Level file loaded successfully".center(100,"="))
            display(self.level_df.head())
        else:
            print("\n"*2 + "item_code column not found. Check level data")     #Raises error if item code not in level file

        #IList of unique item_codes in the current level file
        self.ic_list_df = list(self.level_df['item_code'].unique())
        print(self.ic_list_df)
        
        #Loading hh decile pickle file
        self.decile_df = pd.read_pickle(os.path.normpath(decile_path))
        print("\n"*2 + "Decile file loaded successfully".center(100,"="))
        display(self.decile_df.head())

        #loading level_15 file for HH size
        self.level_15_df = pd.read_stata(level_15_path)
        self.level_15_df = self.level_15_df.pivot(index = "hhid", columns="questionnaire_num", values = 'hh_size')
        self.level_15_df = self.level_15_df[['C','D','F']] #Preserving only FDQ questionnaire hh size
        self.level_15_df.head(10)

        
        #Pivot level_df dataframe
        if cons_column in self.level_df:
            if all([col in self.level_df for col in ['item_code', 'hhid',cons_column]]):
                self.level_df_T = self.level_df.pivot(columns = 'item_code', index = 'hhid', values = cons_column)
                print("\n"*2 + "Level file pivoted successfully.".center(100,"="))
                print(f"Number of distinct items = {self.level_df_T.shape[1]}")
                print(f"Number of distinct hhid in level file = {self.level_df_T.shape[0]}")
                print(f"Total number of distinct hhid = {self.decile_df.shape[0]}")
                display(self.level_df_T.head())
            else:
                print("\n"*2 + f"Item_code/hhid/{cons_column} not in level dta.")
        else: print(f"{cons_column} not in the level_df")

        ##Scaling each item expenditure to hh_size in FDQ questionnaire
        self.hh_scaled_df = self.level_df_T.copy()
        self.hh_scaled_df[self.ic_list_df] = self.hh_scaled_df[self.ic_list_df].\
            apply(lambda col: self.scaling_function(hh_series = col,
                                                    old_hsize_series= self.level_15_df[hh_size_str],
                                                    new_hsize_series = self.level_15_df['F']))
        print("\n"*2 + f"Items in Pivoted file scaled by FDQ hh_size successfully".center(100,"="))
        display(self.hh_scaled_df.head())

        ## Merging scaled pivot table with hh_decile
        self.merged_df = pd.merge(self.decile_df, self.hh_scaled_df,
                                  how = 'outer',
                                  left_index=True, 
                                  right_index=True) 
        print("\n"*2 + "Decile and scaled pivot file merged successfully.".center(100,"="))
        print(f"Check all hhids in merged file are covered? \n{self.merged_df.shape[0] == self.decile_df.shape[0]}")
        display(self.merged_df)

        ##List of item codes in questionnaire
        self.ic_combined = ic_combined

        ## Dataframe with all expenses for 365 days
        self.merged_365_df = self.exp_365(ic_combined, ic_7days, ic_30days, ic_365days)

        ##Final dataframe with weighted average expenditure
        self.final_df = self.wt_avg()

    
    #Scaling function for expenditure by hh_size
    def scaling_function (self, hh_series, old_hsize_series, new_hsize_series):
        """
        Scales each expenditure col from one hh_size (C/D/F) to another of choice.

        Args:
            hh_series: Expenditure series with hhid as index.
            old_hsize_series: original hh_size series with hhid as index.
            new_hsize_series: new hh_size series with hhid as index.
        
        Returns:
            scaled_series: Scaled expeniture with hhid as index.
        """
        scaled_series = hh_series.div(old_hsize_series, axis = 'index')
        scaled_series = scaled_series.mul(new_hsize_series, axis = 'index')
        return scaled_series
    
    # Check if all columns have been covered in the ic list
    def check_ic(self, ic_combined):
        unmatched_ic = set(self.ic_combined)^set(self.ic_list_df)
        return list(unmatched_ic)

    #Scaling expenditure to 365 days
    def exp_365 (self, ic_combined, ic_7days = [], ic_30days = [], ic_365days = []):
        unmatched_ic = self.check_ic(ic_combined)
        if unmatched_ic:
            print(f"There are unmatched codes between questionnaire and table. Please match them first to proceed")
            print(f"Unmatched codes are {unmatched_ic}")
        else:
            print("\n"*2 + "Matching of item codes between dataframe and questionniare successful.".center(120,"=")) 
            condition_list = [np.isin(self.ic_list_df, ic_7days),  # condition for 7 days
                                np.isin(self.ic_list_df, ic_30days), # condition for 30 days
                                np.isin(self.ic_list_df, ic_365days) # condition fo 365 days
            ]
            choice_list = [7,30,365]
            ic_days = np.select(condition_list, choice_list, default=np.nan)

            #Proceed only when ic_days has no nan values
            if np.isnan(ic_days).any():
                print("Some item codes don't have associated day. Allocate days before proceeding")
            else:
                print("All codes have days, proceeding towards converting all expenses to 365 days")
                df = self.merged_df.copy()
                df[self.ic_list_df] = df[self.ic_list_df]*365
                df[self.ic_list_df] = df[self.ic_list_df].div(ic_days)
                df[self.ic_list_df] = df[self.ic_list_df].round(2)
                print("\nAll expenses converted to 365 days")
                display(df.head())
                return df
    
    #Method to calculate weighted average of the group
    def wt_avg (self):
        df = self.merged_365_df.copy()
        df [self.ic_list_df + ['total expenditure']] = df[self.ic_list_df + ['total expenditure']].mul(df['multiplier'], axis = 0)
        mult_sum = df.groupby(by = 'hh_decile', observed = False)['multiplier'].apply('sum')
        sum_df = df.groupby(by = 'hh_decile', observed = False)[self.ic_list_df + ['total expenditure']].apply('sum')
        avg_df = sum_df.div(mult_sum, axis = 0)
        avg_df= avg_df.round(2)
        print("\n"*2 + "Weighted average calculated.".center(100,"="))
        display(avg_df)
        return avg_df


#Executing
r"""if __name__ == "__main__":
    
    # Take user inputs
    #level_path = input("Enter the path of the level file:")
    level_path = r"G:\.shortcut-targets-by-id\1NprIdwv7vnADEhIOU_jU6YsVBoFvikqW\Coal Research\HCES 2022-23\Dta raw files\level_10.dta"
    level_15_path = r"G:\.shortcut-targets-by-id\1NprIdwv7vnADEhIOU_jU6YsVBoFvikqW\Coal Research\HCES 2022-23\Dta raw files\level_15.dta"
    decile_path = r"G:\.shortcut-targets-by-id\1NprIdwv7vnADEhIOU_jU6YsVBoFvikqW\Coal Research\HCES 2022-23\Python implementation\Codes\HH decile level expenditure\hh_decile.pkl"
    r_ic_combined = ['300', '301', '302', '309'] + ['310', '311', '316', '312', '314', '315', '313', '317', '319'] + ['322', '324', '323', '321', '320', '325', '329']
    #decile_path = input("Enter the path of the decile file: ")

    # Creating an instance of the path
    level_10 = hh_decile_exp( current_level_path= level_path, 
                             decile_path= decile_path,
                             ic_combined=r_ic_combined, 
                             ic_7days= r_ic_combined,
                             level_15_path=level_15_path,
                             hh_size_str ='C')"""