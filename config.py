import os
from dotenv import load_dotenv

load_dotenv()

R_APP_KEY = os.getenv("M_APP_KEY")
R_APP_SECRET = os.getenv("M_APP_SECRET")
M_APP_KEY = os.getenv("M_APP_KEY")
M_APP_SECRET = os.getenv("M_APP_SECRET")
ACCOUNT_NUMBER = os.getenv("ACCOUNT_NUMBER")

