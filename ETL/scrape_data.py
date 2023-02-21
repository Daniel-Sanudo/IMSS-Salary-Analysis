from selenium import webdriver
from splinter import Browser
from webdriver_manager.chrome import ChromeDriverManager
import datetime as dt
import logging
import sys
import time
import os

# Create logger basic config
logging.basicConfig(handlers=[logging.FileHandler(filename="scrape_data.log",
                                                 encoding='windows-1252', mode='w')],
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

# Create logger object that will be used to debug the app
logger = logging.getLogger(__name__)

# Create selenium log to avoid cramming the debug log with selenium messages
selenium_log = logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.ERROR)

# Create urllib3 log to avoid cramming the debug log with urllib3 messages
urllib3_log = logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)


def scrape_year(target_year,target_month='All'):
    
    # Determine directory path
    download_path = os.path.join(os.getcwd(),'IMSS_Files')
    
    # Create download directory in case it doesnt exist
    os.makedirs(download_path,
               exist_ok = True)
    
    logger.debug(f'Downloading files into {download_path}')

    # Create options chrome options instance
    options = webdriver.ChromeOptions()
    
    # Create options dictionary
    prefs = {"download.default_directory" : f'{download_path}',
            "download.directory_upgrade": "true",
            "download.prompt_for_download": "false",
            "disable-popup-blocking": "false"}
    
    # Add options dictionary to options instance
    options.add_experimental_option("prefs", prefs)
    
    # Install chrome driver manager
    executable_path = {'executable_path': ChromeDriverManager().install()}
    
    # Initiate automated browser with preferred options
    browser = Browser('chrome', **executable_path, headless=True, options=options)
    
    scrape_month(browser,target_year,target_month, download_path)

def scrape_month(browser,year,month, path):
    
    # Get the list of files that were already downloaded in the download path
    previously_downloaded_files = os.listdir(path)

    logger.debug(f'The directory had these files previously: {previously_downloaded_files}')
    
    # Define function to verify if all downloads were completed
    def download_status():
        # Create loop to verify the download status of the file without having to access the
        # shadow-root of chrome downloads
        while True: 
            # Wait 5 seconds to confirm the newly downloaded file shows up
            time.sleep(5)
            
            # Get a list of all the files in the download path including the new ones
            all_downloaded_files = os.listdir(path)
            
            # Remove the files that were previously downloaded from the list
            new_downloaded_files = [x for x in all_downloaded_files 
                                    if x not in previously_downloaded_files]

            logger.debug(f'These are the new files {new_downloaded_files}')
            
            # Get a list of the files that are pending download
            pending_downloads = [x for x in new_downloaded_files
                                    if 'crdownload' in x]
            
            # Verify if all files finished downloading to quit the browser 
            if len(pending_downloads) == 0:
                break
    
    # Set the target url according to the year argument
    target_url = f'http://datos.imss.gob.mx/dataset/asg-{year}'
    
    # Acess the url for the target year
    browser.visit(target_url)
    
    # Validate if there's a specific target month or if all files will be downloaded
    if month == 'All':
        
        # Create list to store the list of extracted urls as strings
        links_for_each_month = []
        
        # Loop through the urls that match the search text asg-{year}
        for link in browser.links.find_by_partial_text(f'asg-{year}'):
            
            # Append the link as a string to avoid stale element error
            links_for_each_month.append(str(link['href']))
        
        # Loop through the link list given by links_for_each_month
        for link in links_for_each_month:
            
            # Visit the new url
            browser.visit(link)
            
            # Look for the url that includes node in its path to find the csv
            for download_link in browser.links.find_by_partial_href('node'):
                
                # Store the link as a string to avoid stale element error
                csv_link = str(download_link['href'])
            
            # Visit the path with link which will be downloaded
            browser.visit(csv_link)
            
            # Find the download button and click it
            browser.links.find_by_partial_href('download').click()
            
            # Wait for the file to finish downloading before moving to the next one
            download_status()
            
        # Close the browser
        browser.quit()
        logger.info(f'Finished scraping files for {year}')
        
    # Download only the csv file for the target month
    else:
        
        # Cast month as a string
        month = str(month)
        
        # Verify that the target month begins with 0 in case it's below 10
        if month[0] != 0 and len(month)<2:
            
            # Add a 0 before the target month
            month = f'0{month}'
            
        # Append the link as a string to avoid stale element error
        link_for_target_month = str(browser.links.find_by_partial_text(f'asg-{year}-{month}')['href'])
        
        # Visit the new url
        browser.visit(link_for_target_month)   
        
        # Find the download button and click it
        browser.links.find_by_partial_href('download').click()
        
        # Track the download status
        download_status()
        
        # Close the browser
        browser.quit()

def main():
    if len(sys.argv) == 2:
        if int(sys.argv[1]) >= 1997 and int(sys.argv[1]) <= dt.date.today().year:
            scrape_year(int(sys.argv[1]))
        else:
            logger.error(f'Invalid year, make sure it is between 1997 and {dt.date.today().year}')
    else:
        logger.error(f'Invalid input, make sure its a 4 digit year between 1997 and {dt.date.today().year}')

if __name__ == "__main__":
    main()





