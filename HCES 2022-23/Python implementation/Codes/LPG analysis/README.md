# About [WiP]
Objective -- Identify major determinants of LPG usage in India.<br><br>
Method -- Fit a random forest on LPG classes using socio-economic data.<br><br>
Key findings -- Random Forest model achieved ~54 per cent accuracy in both CV and test dataset analysis. Very good recall (0.94) for HHs with No LPG consumption. May be we should restrict ourselves to only no LPG and exclusive LPG HHs. Key features associated with the classification include: fridge ownership, tv ownership, monthly per capita expenditure, floor material, and sector (rural vs. urban). 


# Repo structure
1. `New_encodings.txt`: Labels used for missing data in HCES 2022-23.
2. `Column_labels.txt`: Contains names of the columns used in the `Raw_data.dta` (see below). Also, specifies its location in the HCES questionnaire.
3. `Extraction.iPYNB`: Notebook file to extract and export relavant information from HCES.
4. `Raw_data.dta`: Output of `Extraction.iPYNB`. Contains the raw data used for classification. It has not been uploaded on github due to file constraints. Available on request.
5. `Random_Forest_analysis.iPYNB`: Contains the code tune hyperparameters for a Random Forest model that predicts whether a household is primary, secondary, exclusive, or no LPG consumer.