## About
This repo calculates quintile-wise average Monthly Per Capita Expenditure for each of the 381 commodities in HCES 2022-23. 

## Repo structure
This repo consists of the following major files:
1. `HH_quintile.iPYNB`: Notebook to assign households to quintiles based on total expenditure.
2. `hh_quintile.pkl`: Stores results from the file in step-1.
3. `Average.py`: Generalized script to calculate quintile-wise expenditure on each commodity in a given level in HCES 2022-23. 
4. `Level wise quintile exp.iPYNB`: Uses 'Average.py' to calculate quinitile-wise expenditure for each level and commodity in HCES 2022-23.
5. `lvl5_quintile to lvl13_quintile.pkl`: Level wise results from step-4.
6. `Merging final quintiles.iPYNB`: Notebook to merge level-wise results into a single dataframe.
7. `TM - Item-wise hh quintile expenditure 15Jan24.xlsx`: Contains the merged quintile-wise expenditure for all commodities.