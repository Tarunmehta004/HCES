## About
HCES 2023-24 interviews ~2,62,000 households across India to collect information on their expenditures on food, consumable and durable commodities. The raw data is spread across 15 different files. This repo aggregates all the expenses to arrive at Monthly Per Capita Expenditure (MPCE) for each household and assigns them to a decile, separately for Rural and Urban areas, based on their relative position in the overall distribution.

You can find more information about the HCES 2023-24 [here](https://www.mospi.gov.in/sites/default/files/publication_reports/Final_Report_HCES_2023-24L.pdf).


## Repo structure
This repo consists of the following files of interest:
1. `MPCE.iPYNB`: Contains python code to compute MPCE and assign households to deciles.
2. `MPCE 2023_24 2Apr25.dta`: Contains the computed MPCE and decile assignment. 