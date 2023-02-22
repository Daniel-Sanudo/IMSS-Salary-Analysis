#!/bin/bash

# Function to validate if the year is between 1997 and current year
function validate_year {
    year=$1
    current_year=$(date +"%Y")
    if [[ $year -lt 1997 ]] || [[ $year -gt $current_year ]]; then
        echo "Invalid year. Please enter a year between 1997 and $current_year."
        exit 1
    fi
}

# Prompt user for year range
read -p "Enter the starting year for the range: " start_year
validate_year $start_year

read -p "Enter the ending year for the range: " end_year
validate_year $end_year

# Prompt user for target state
read -p "Enter the target state (a number between 1 and 32): " target_state
if [[ $target_state -lt 1 ]] || [[ $target_state -gt 32 ]]; then
    echo "Invalid state. Please enter a number between 1 and 32."
    exit 1
fi

# Loop through the year range and call cleanse_data.py and scrape_data.py for each year
for (( year=$start_year; year<=$end_year; year++ )); do
    echo "Processing data for year $year"
    python cleanse_data.py $target_state
    python scrape_data.py $year
done

echo "All data processing completed successfully."