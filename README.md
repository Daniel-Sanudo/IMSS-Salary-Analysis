# IMSS-Salary-Analysis
Investigation project to find trends in the IMSS job dataset pertaining to job offers and their salaries

## ETL Files
There are two python scripts inside the ETL directory: cleanse_data.py and scrape_data.py. In order to run them one can execute the run_ETL_pipeline.sh file and the file will ask for the appropriate arguments, if only one of the scripts needs to be executed follow the ntext instructions:

### cleanse_data
cleanse_data.py takes the additional argument of the target state as its second parameter. The target state and its respective code can be found inside EDA/IMSS_state_code_to_state, for example:
```
python cleanse_data.py 1
```

### scrape_data
scrape_data.py takes the additional argument of the target year as its second parameter. The target year must be between 1997 and the current year, for example:
```
python scrape_data.py 2022
```

