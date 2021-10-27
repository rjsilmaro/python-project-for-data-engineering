import glob                         # this module helps in selecting files
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml_personal_details(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe

def extract_from_xml_dealership(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        dataframe = dataframe.append({"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price, "fuel":fuel}, ignore_index=True)
    return dataframe

def extract(path, type):
    if type == "dealership":
        extracted_data = pd.DataFrame(
            columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])  # create an empty data frame to hold extracted data

        # process all xml files
        for xmlfile in glob.glob(path + "*.xml"):
            extracted_data = extracted_data.append(extract_from_xml_dealership(xmlfile), ignore_index=True)
    elif type == "personal details":
        extracted_data = pd.DataFrame(
            columns=['name', 'height', 'weight'])  # create an empty data frame to hold extracted data

        # process all xml files
        for xmlfile in glob.glob(path + "*.xml"):
            extracted_data = extracted_data.append(extract_from_xml_personal_details(xmlfile), ignore_index=True)
    else:
        log("Unsupported. Data is neither personal details nor dealership")
        exit()

    # process all csv files
    for csvfile in glob.glob(path+"*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)

    # process all json files
    for jsonfile in glob.glob(path+"*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)

    return extracted_data

def transform(data, type):
    if type == "dealership":
        data['price'] = round(data.price, 2)
        return data
    elif type == "personal details":
        # Convert height which is in inches to millimeter
        # Convert the datatype of the column into float
        # data.height = data.height.astype(float)
        # Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
        data['height'] = round(data.height * 0.0254, 2)

        # Convert weight which is in pounds to kilograms
        # Convert the datatype of the column into float
        # data.weight = data.weight.astype(float)
        # Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
        data['weight'] = round(data.weight * 0.45359237, 2)
        return data
    else:
        exit()

def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)

def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    #timestamp_format = '%H:%M:%S-%h-%d-%Y'  # Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("resources/log files/logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

def process_etl(targetfile, path, type):
    log("Extract phase Started")
    extracted_data = extract(path, type)
    log("Extract phase Ended")

    log("Transform phase Started")
    transformed_data = transform(extracted_data, type)
    log("Transform phase Ended")

    log("Load phase Started")
    load(targetfile, transformed_data)
    log("Load phase Ended")

def start_etl_for_personal_details():
    tmpfile = "resources/personal details/temp.tmp"  # file used to store all extracted data
    logfile = "resources/log files/logfile.txt"  # all event logs will be stored in this file
    targetfile = "resources/transformed files/transformed_data.csv"  # file where transformed data is stored
    path = "resources/personal details/"
    type = "personal details"

    log("ETL Job for Personal Details Started ")

    process_etl(targetfile, path, type)

    log("ETL Job for Personal Details Ended")


def start_etl_for_dealership():
    tmpfile = "resources/dealership/dealership_temp.tmp"  # file used to store all extracted data
    logfile = "resources/log files/logfile.txt"  # all event logs will be stored in this file
    targetfile = "resources/transformed files/dealership_transformed_data.csv"  # file where transformed data is stored
    path = "resources/dealership/"
    type = "dealership"

    log("ETL Job for Dealership Started ")

    process_etl(targetfile, path, type)

    log("ETL Job for Dealership Ended")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    log("\n")

    try:
        start_etl_for_personal_details()
    except Exception as e:
        log("Failed: " + str(e))

    try:
        start_etl_for_dealership()
    except Exception as e:
        log("Failed: " + str(e))


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
