# BDE-Project

## Setup of a data Pipeline between velib API and Google BigQuery

The point of this project is to build a pipeline using Google Cloud (BigQuery, Airflow) that link velib's API and Google BigQuery, while also formating the data from a semi-structured format to a structured format.

### Velib API 

Description of the API : 
https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole

Request to have number of bikes availables by station id :  
https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json

Request to have the location, name of the station by station id :
https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json



### Steps of the project 

Firstly, we had to create our own cluster on Google Cloud.

![image](https://user-images.githubusercontent.com/61540992/210100245-05a2c5ce-29e5-4198-a3fa-627bd20ae22f.png)

#### Configuration of the DAG

Then we can focus on creating our first DAG (Code available in the git repo): 

![image](https://user-images.githubusercontent.com/61540992/210100350-ccca5a4a-5803-4c00-81da-f9875e99d6c9.png)


Our DAG is composed of 3 différent tasks : 

  `Fetch_JSON` : The code defines a function fetch_json that sends an HTTP GET request to the specified URL using the requests library, then saves the response in JSON format to a file  called "data.json". This way we can periodically fetch data from the API to get the newest data for our analysis.
  
  `Convert_JSON_To_CSV` : The code loads the JSON file into a Pandas DataFrame, performs some data transformation and manipulation operations on the DataFrame ( remove duplicate/useless columns), and then saves the resulting DataFrame as a CSV file.
  
  `Load_To_BigQuery` : We then run a bash command to load the data into Google BigQuery, and save it in the "data" folder of the cluster.

`fetch_json_task >> json_to_csv_task >> load_task`



Before running the DAG, we had to create our dataset and create/configure our table in BigQuery. All values types are set to NUMERICAL.
As we can see in BigQuery the data was transformed and loaded correctly, it can then be used in a PowerBI project to perform analysis.

*Structure of our dataset* :

![image](https://user-images.githubusercontent.com/61540992/210101323-db5ca082-e011-411e-bffc-8a10558a4ccf.png)


We can also check that our Dag is running correctly through Apache Airflow interface

![image](https://user-images.githubusercontent.com/61540992/210101243-29cf7ce3-3476-48d9-b1da-56c4d7612809.png)

### Visualization on Power BI 
#### By connecting Big Query and Power BI

Report made with PowerBI, after connecting BigQuery & PowerBI (Available in the github): 

![image](https://user-images.githubusercontent.com/61540992/210114705-533b2bac-fafb-4a89-a671-84c9bb909a10.png)
