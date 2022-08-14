# import libraries 
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import random
import pymysql
from datetime import timedelta
from datetime import date
import re

#----------------------------------------------------------------------
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
#-----------------------------------------------------------------------------------------
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
#-------------------------------------------------------------------------------------
from selenium.webdriver.support.ui import WebDriverWait  # for implicit and explict waits
from selenium.webdriver.chrome.options import Options  # for suppressing the browser
#------------------------------------------------------------
#Initializing the webdriver

chrome_service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())

option = webdriver.ChromeOptions()
option.add_argument('headless')
driver = webdriver.Chrome( service=chrome_service, options= option  )
driver.implicitly_wait(70)



#------------------------------------------------------------
# importing the global file that has information about each of the countries which have Indeed
worldwidelinks = pd.read_csv ('worldwidelink.csv').set_index ('CountryName')
#------------------------------------------------------------
#Initializing the main variables
filterdate = 1
position = 'data'
totalpostion = 0
#------------------------------------------------------------
# Collecting all pages links for each country in the dataset
def collectinglinks ():
  print ("Start collecting links")
  totalpostion = 0
  Links = []
  for country in worldwidelinks.index[1:10]:
    try :
        # preparation the link
        url = worldwidelinks['WebURL'].loc[country]+'jobs?q={}&fromage={}'.format (position ,filterdate )
        driver.get(url)    
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        #Gathering the baseline information
        cards = len (soup.find_all('div', 'cardOutline'))
        totalpostion = totalpostion + cards
        #--------------------------------------------------------
        # Some pages dosn't have any information
        try:
            page = soup.find("div", id ="searchCountPages").get_text().strip()
        except:
            print ('no information')
        #--------------------------------------------------------
        # Apped the URL to the our list
        if cards > 0 : Links.append ({'country': country, 'URL':url , 'Position':cards})
        #--------------------------------------------------------
        # now we are working on gathering the other pages links if exist so WHILE Loop will still true if there are more pages
        while True:
            try:
                url2 =  worldwidelinks['WebURL'].loc[country] + soup.find('a', {'aria-label':worldwidelinks['last_page'].loc[country]}).get('href')
            except AttributeError:
                break
            #--------------------------------------------------------
            # Now we are using Beautifulsoup to get the number of results inside this page
            driver.get(url2)    
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'lxml')
            cards = len (soup.find_all('div', 'cardOutline'))
            totalpostion = totalpostion + cards
            #--------------------------------------------------------
            # Some pages dosn't have any information inside this page
            try :
                page = soup.find("div", id ="searchCountPages").get_text().strip()
            except :
                print ('no information')
            #--------------------------------------------------------
            # Print the URL and the number of results and apped the URL's to the LIST
            if cards > 0 : Links.append ({'country': country, 'URL':url2, 'Position':cards})
            print (country ,"   ",cards,"   ")
    except :
        print ("Error 1701" , country)
        
  lldata = pd.DataFrame (Links)
  dt_string = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
 # file_name = str (dt_string) +"--" + str(int(random.random()*12345)) 

  try:
    lldata.to_excel ('1linkdataset.xlsx')
  except:
   lldata.to_csv ('1linkdataset.csv')

  return (Links)
#------------------------------------------------------------------------------------------------------------------

def fulldesc (link ):
    try:
        driver.get(link)    
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        text = soup.get_text()
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
    except:
        text = "Error in colecting the data"
        print ("Error 1702" , link)
    return(text) 
#------------------------------------------------------------------------------------------------------------------
def gatheringdata (pagelinks):
  Dataset2 = []
  num = 0
# Gathering data from links
  for link in pagelinks:
      #--------------------------------------------------------
      # Now we are using Beautifulsoup to get the number of results
      try:
          driver.get(link['URL'])    
          page_source = driver.page_source
          soup = BeautifulSoup(page_source, 'lxml')
          Posted_Date = 0
          #--------------------------------------------------------
          # Now we are using Beautifulsoup to get interested information
          job_title = soup.find_all("h2", class_="jobTitle")
          companyName = soup.find_all('span', 'companyName')
          companyLocation = soup.find_all('div', 'companyLocation')
          des = soup.find_all('div', 'job-snippet')
          dateee = soup.find_all('span', 'date')
          job_url = soup.find_all('a', class_ = 'jcs-JobTitle css-jspxzf eu4oa1w0')
          RatingNumber = soup.find_all("span", class_="ratingNumber")
          salary = soup.find_all("div", class_="metadata salary-snippet-container")
          job_type = soup.find_all("div", class_="attribute_snippet")
          job_id = soup.find_all("h2", class_="jobTitle jobTitle-newJob css-bdjp2m eu4oa1w0")
          #--------------------------------------------------------
          # Now we are going deepth inside the page to collect our date    
          for i  , b in enumerate (dateee):
              num = num + 1
              try :
                  Job_ID = str (job_id[i])[str (job_id[i]).find ("data-jk=")+9 : str (job_id[i]).find ("data-jk=")+25]
              except:
                  Job_ID = "N/A"
              try :
                  test = job_type[i].get_text()
                  if any(chr.isdigit() for chr in test) == True :
                      Job_type = 'N/A'
                  else:
                      Job_type = test
              except:
                  Job_type = 'N/A'
              try :
                  Rating_Number = RatingNumber[i].get_text()
              except:
                  Rating_Number = 'N/A'
              try :
                  Salary = salary[i].get_text()
              except:
                  Salary = 'N/A'
              try:
                  Job_title = job_title[i].get_text()
              except:
                  Job_title = 'N/A'    
              try:
                  CompanyName = companyName[i].get_text().strip()
              except:
                  CompanyName = 'N/A'
              try:
                  CompanyLocation=companyLocation[i].get_text().strip()
              except:
                  CompanyLocation = 'N/A'     
              try: 
                  Job_discribtion = des[i].get_text().strip()
              except:
                  Job_discribtion = 'N/A'
              try: 
                Datee = dateee[i].get_text().strip()
              except:
                Datee = 'N/A' 
              try:
                exdate= [int(x) for x in re.findall(r'-?\d+\.?\d*',Datee)][0]
                Posted_Date = date.today() - timedelta(days= exdate )
              except:
                Posted_Date = date.today()
              try:
                  job_url =  link['URL'][0:22]+"viewjob?jk="+ Job_ID
              except:
                  job_url = 'N/A'
              try :
                fulljobdescribtion = fulldesc (job_url)
              except:
                fulljobdescribtion = "N/A"
              Dataset2.append( {"Countrye" :link['country'] ,
                                "city" : CompanyLocation,
                                'JobId':Job_ID ,
                                'Source':'Indeed' ,
                                'CollectedDate' :datetime.today().strftime('%Y-%m-%d') , 
                                "JobTitle":Job_title , 
                                "CompanyName" :CompanyName , 
                                'RatingNumber':Rating_Number,
                                "PostedDate":Datee ,
                                'Salary':Salary  ,
                                'JobType':Job_type ,
                                "jobURL" : job_url , 
                                "ShortDiscribtion" : Job_discribtion  ,                         
                                'fullJobDescribtion' : fulljobdescribtion  ,
                                 "Posted_Date_N" :Posted_Date} )
            
      except:
          print ("Error 1703" , link)
  return (Dataset2)




def main():
    dd = collectinglinks()
    ff = gatheringdata(dd)
    df2 = pd.DataFrame(ff)
    
    my_conn = create_engine("mysql+pymysql://admin:12345678@database-1.ciaff8ckhmlj.us-west-2.rds.amazonaws.com:3306/IndeedDataBase")
    df2.to_sql (con =my_conn , name = 'IndeedDataSet5' , if_exists = 'append' , index = False )
    
    dt_string = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    
    #file_name = str ( dt_string ) +"--" + str(int(random.random()*12345)) 
    
    try:
      df2.to_excel('1dataset.xlsx')
    except:
      df2.to_excel('1dataset.csv')  

    
if __name__ == '__main__':
  main()
