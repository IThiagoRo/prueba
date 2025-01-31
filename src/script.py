from components.data_ingestion import DataIngestion
from components.data_transformation import DataTransformation
from components.output import Output

def get_data():
    ingestion = DataIngestion()
    ingestion.initiate_data_ingestion()
    return ingestion

def transformation_of_data(ingestion):
    transformation = DataTransformation()
    transformation.db = ingestion.db
    transformation.initiate_data_transformation()
    return transformation

def export_data_to_db(transformation):
    out = Output()
    out.db = transformation.new_db
    out.initiate_output()

def main():
    ingestion = get_data()
    transformation = transformation_of_data(ingestion)
    export_data_to_db(transformation)


if __name__ == '__main__':
    main()
