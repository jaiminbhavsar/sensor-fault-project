from sensor.configurartion.mongo_db_connection import MongoDBClient
from sensor.entity.config_entity import TrainingPipelineConfig,DataIngestionConfig
from sensor.pipeline.training_pipeline import TrainPipeline
from sensor.logger import logging
import os
from sensor.utils.main_utils import read_yaml_file
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sensor.constant.application import APP_PORT,APP_HOST
from uvicorn import run as app_run
from fastapi.responses import Response
from fastapi.responses import RedirectResponse


env_file_path=os.path.join(os.getcwd(),"env.yaml")

def set_env_variable(env_file_path):
    if (os.getenv("MONGO_DB_URL",None)) is None:
        env_config = read_yaml_file(env_file_path)
        os.environ['MONGO_DB_URL']= env_config['MONGO_DB_URL']

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/train")
async def train_route():
    try:
        train_pipeline = TrainPipeline()
        if train_pipeline.is_pipeline_running:
            return Response("Training pipeline is already running.")
        train_pipeline.run_pipeline()
        return Response("Training successful !!")
    except Exception as e:
        return Response(f"Error Occurred! {e}")


@app.get("/", tags=["authentication"])
async def index():
    return RedirectResponse(url="/docs")

# def main():
#     try:
#         train_pipeline = TrainPipeline()
#         logging.info("Start running the pipeline")
#         train_pipeline.run_pipeline()
#     except Exception as e:
#         print(e)
#         logging.exception(e)


if __name__ == '__main__':
    app_run(app,host=APP_HOST,port=APP_PORT)        


