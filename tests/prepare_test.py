from app.config import ConfigClass
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.main import create_app


class SetupTest:
    def __init__(self, log):
        self.log = log
        app = create_app()
        self.client = TestClient(app)
        self.log.info("Test Start")
