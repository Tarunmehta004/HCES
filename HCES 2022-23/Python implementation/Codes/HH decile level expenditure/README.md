## About
This repo calculates decile-wise average Monthly Per Capita Expenditure for each of the 381 commodities in HCES 2022-23. 

## Repo structure
This repo consists of the following major files:
1. `1_HH_decile.iPYNB`: Notebook to assign households to decile based on total expenditure.
2. `hh_quintile.pkl`: Stores results from the file in step-1.
3. `Average.py`: Generalized script to calculate decile-wise expenditure on each commodity in a given level in HCES 2022-23. 
4. `2_Level wise decile exp.iPYNB`: Uses 'Average.py' to calculate quinitile-wise expenditure for each level and commodity in HCES 2022-23.
5. `lvl5_quintile to lvl13_quintile.pkl`: Level wise results from step-4.
6. `3_Merging final decile.iPYNB`: Notebook to merge level-wise results into a single dataframe.
7. `TM - Item-wise hh decile expenditure (compiled) 8Jan24.xlsx`: Contains the merged decile-wise expenditure for all commodities.