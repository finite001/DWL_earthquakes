# DWL Earthquakes
## Contributors: Sandro Huber, Lea Senn, Thomas Schwendimann

### Project Description
This project of the module Data Warehouse and Data Lake I + II aims to use Twitter as an early warning system for earthquakes, using Twitter API and earthquake.usgs API as data sources, and various technologies from Amazon Web Services, such as Apache Airflow and PostgreSQL. The results are visualized with Tableau. The details of this project can be found in the folder docs, where the whole project is documented. The following image showcases the architecture of our project.


![Architecture](img/architecture.png)

### Structure

DAG: Contains the final code for the pipeline

Jupyter Notebook: Contains local prototypes to test our code. 

Python Scripts: Contains local prototypes to test our code

Unused: Contains scripts that were not used in the final project

docs: Contains project documentation



### Gitignore
Currently ignores the following:

.txt # tweets which were saved locally for testing purposes

.env # environment variables with credentials

.csv

.ipynb_checkpoints