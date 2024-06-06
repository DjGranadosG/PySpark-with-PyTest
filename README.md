# PySpark with PyTest - Beta Version

This is a small example of using PySpark with PyTest. The purpose of this project is to demonstrate how we can use PySpark for data transformations and how to test these transformations using PyTest.

## Description

The project uses a dataset of crimes in Chicago that can be downloaded from [Data Portal of Chicago](https://data.cityofchicago.org/). This dataset is processed using PySpark, and the transformations performed are tested using PyTest.

## Requirements

- Python 3.8+
- PySpark
- PyTest
- requests
- python-dotenv

## Installation

1. Clone this repository:

   ```sh
   git clone <repository URL>
   cd <repository name>
   
2. Create and activate a virtual environment:

   ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   
3. Install the dependencies:

   ```sh
    pip install -r requirements.txt

## Configuration

1. Create a .env file in the root directory of the project with the following variables. Here you have an example:

   ```sh
    SPARK_APP_NAME=FirstApproachSpark
    SPARK_URL=local[3]
    SPARK_EXECUTOR_MEMORY=1g
    SPARK_EXECUTOR_CORES=3
    SPARK_CORES_MAX=3
    DATA_URL=C:/Proyect_Name/Chicago_crime_data.csv
    REMOTE_DATA_URL=https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD
    LOCAL_FILENAME=Chicago_crime_data.csv

## Usage

1. To run the main script and perform data transformations, execute:

   ```sh
    python main.py
   
## Author

David Granados

## License

This project is licensed under the MIT License.