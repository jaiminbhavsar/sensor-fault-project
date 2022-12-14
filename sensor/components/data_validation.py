from sensor.exception import SensorException
from sensor.logger import logging
from sensor.entity.config_entity import DataValidationConfig
from sensor.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
import pandas as pd
from pandas import DataFrame
from sensor.utils.main_utils import read_yaml_file,write_yaml_file
from sensor.constant.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import os,sys

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_config=DataValidationConfig):
        try:
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise SensorException(e,sys)

    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            numerical_column_present = True
            missing_numerical_columns = []
            for num_col in numerical_columns:
                if num_col not in dataframe_columns:
                    numerical_column_present = False
                    missing_numerical_columns.append(num_col)
            logging.info(f"missing numerical columns[{missing_numerical_columns}]")

            return numerical_column_present
        except Exception as e:
            raise SensorException(e,sys)

    def is_numerical_column_exist(self,dataframe:pd.DataFrame)->bool:
        try:
            number_columns = self._schema_config["columns"]
        except Exception as e:
            raise SensorException(e,sys)


    @staticmethod
    def read_data(filepath)->pd.DataFrame:
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise SensorException(e,sys)


    def detect_datasetdrift(self,base_dataframe,current_dataframe,threshold=0.05)->bool:
        try:
            status = True
            report = {}
            list_column_baseDataframe = base_dataframe.columns
            list_column_currentDataframe = current_dataframe.columns

            for column in list_column_baseDataframe:
                d1 = base_dataframe[column]
                d2 = current_dataframe[column]
                is_same_distribution= ks_2samp(d1,d2)

                if is_same_distribution.pvalue >= threshold:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column:{
                    "pvalue":float(is_same_distribution.pvalue),
                    "drift_status":is_found
                }
                })
            
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            #Create directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok = True)
            write_yaml_file(file_path=drift_report_file_path,content=report)

            return status
        except Exception as e:
            raise SensorException(e,sys)
        
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            error_message = ""
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            #Read Data from train and test file
            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            # Validate the columns
            status = self.validate_number_of_columns(train_dataframe)
            if not status:
                error_message = f"{error_message} Train DataFrame does not contain all the columns"
            
            status = self.validate_number_of_columns(test_dataframe)
            if not status:
                error_message = f"{error_message} Test DataFrame does not contain all the columns"
            
            #Validate numerical columns
            status = self.validate_number_of_columns(dataframe=train_dataframe)

            if not status:
                error_message = f"{error_message} Train data frame does not contain all the numerical columns"

            status = self.validate_number_of_columns(dataframe=test_dataframe)
            if not status:
                error_message = f"{error_message} Test data frame does not contain all the numerical columns"

                
            if len(error_message)>0:
                raise Exception(error_message)

            status = self.detect_datasetdrift(base_dataframe=train_dataframe,current_dataframe=test_dataframe,threshold=0.05)

            data_validation_artifact = DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )

            logging.info(f"Data validation artifact: {data_validation_artifact}")

            return data_validation_artifact

        except Exception as e:
            raise SensorException(e,sys)

