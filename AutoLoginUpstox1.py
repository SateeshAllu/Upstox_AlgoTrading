import requests as rq
from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse as urlparse
from pyotp import TOTP
import os
import certifi
import logging
import ssl
import warnings
import shutil
import json
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from urllib3.exceptions import InsecureRequestWarning
from datetime import datetime

# Configure logging to DEBUG level
log_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'auth_log_{datetime.now().strftime("%Y%m%d")}.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Log to terminal as well
    ]
)
logger = logging.getLogger(__name__)

# Custom SSL adapter class
class CustomSSLAdapter(HTTPAdapter):
    def __init__(self, certfile=None, *args, **kwargs):
        self.certfile = certfile
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.certfile:
            ctx.load_verify_locations(cafile=self.certfile)
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
            **pool_kwargs
        )

# Path to your downloaded certificate files
CERT_FILE_PATHS = [
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "Amazon Root CA 1.crt"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "Zscaler Root CA.crt")
]

# Function to create a custom cert bundle with multiple certificates
def create_custom_cert_bundle():
    try:
        for cert_path in CERT_FILE_PATHS:
            if not os.path.exists(cert_path):
                logger.error(f"Certificate file not found at {cert_path}")
                return None

        custom_bundle_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "custom_cert_bundle.pem")
        shutil.copy(certifi.where(), custom_bundle_path)
        
        for cert_path in CERT_FILE_PATHS:
            with open(cert_path, "rb") as cert_file:
                with open(custom_bundle_path, "ab") as bundle_file:
                    bundle_file.write(b"\n")
                    bundle_file.write(cert_file.read())
            logger.info(f"Appended certificate from {cert_path} to custom bundle")
        
        logger.info(f"Created custom certificate bundle at {custom_bundle_path}")
        return custom_bundle_path
    except Exception as e:
        logger.error(f"Error creating custom certificate bundle: {str(e)}")
        return None

# Create a session with the custom certificate
def create_ssl_session():
    try:
        custom_bundle = create_custom_cert_bundle()
        session = rq.Session()
        
        if custom_bundle and os.path.exists(custom_bundle):
            adapter = CustomSSLAdapter(certfile=custom_bundle)
            session.mount('https://', adapter)
            session.verify = custom_bundle
            logger.info("Using custom certificate bundle for HTTPS requests")
        else:
            warnings.filterwarnings('ignore', category=InsecureRequestWarning)
            adapter = CustomSSLAdapter()
            session.mount('https://', adapter)
            session.verify = False
            logger.warning("SSL verification disabled - no valid certificate found")
        
        return session
    except Exception as e:
        logger.error(f"Error creating SSL session: {str(e)}")
        session = rq.Session()
        session.verify = False
        return session

# Load credentials from JSON file
try:
    with open('credentials.json', 'r') as file:
        creds = json.load(file)
    api_key = creds['api_key']
    secret_key = creds['secret_key']
    r_url = creds['redirect_uri']
    totp_key = creds['totp_key']
    mobile_no = creds['mobile_no']
    pin = creds['pin']
    logger.info(f"API Key: {api_key}")
    logger.info(f"Secret Key: {secret_key}")
    logger.info(f"Redirect URL: {r_url}")
except Exception as e:
    logger.error(f"Failed to load credentials from credentials.json: {str(e)}")
    raise

auth_url = f'https://api-v2.upstox.com/login/authorization/dialog?response_type=code&client_id={api_key}&redirect_uri={r_url}'
logger.info(f"Auth URL: {auth_url}")

# Main script
print(os.getcwd())

try:
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    # options.add_argument('--headless')  # Uncomment if you want to run headless
    driver = webdriver.Chrome(options=options)
except Exception as e:
    logger.error(f"Failed to initialize Selenium: {str(e)}")
    raise

try:
    driver.get(auth_url)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="text"]'))).send_keys(mobile_no)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="getOtp"]'))).click()
    totp = TOTP(totp_key).now()
    sleep(2)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="otpNum"]'))).send_keys(totp)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="continueBtn"]'))).click()
    sleep(2)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="pinCode"]'))).send_keys(pin)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="pinContinueBtn"]'))).click()
    sleep(2)

    token_url = driver.current_url
    parsed = urlparse.urlparse(token_url)
    code = urlparse.parse_qs(parsed.query)['code'][0]
    logger.info(f"Authorization code obtained: {code}")
except Exception as e:
    logger.error(f"Failed during Selenium authentication: {str(e)}")
    driver.close()
    raise
finally:
    driver.close()

url = 'https://api-v2.upstox.com/login/authorization/token'
headers = {
    'accept': 'application/json',
    'Api-Version': '2.0',
    'Content-Type': 'application/x-www-form-urlencoded'
}

data = {
    'code': code,
    'client_id': api_key,
    'client_secret': secret_key,
    'redirect_uri': r_url,
    'grant_type': 'authorization_code'
}

# Use the custom SSL session for the token request
session = create_ssl_session()
try:
    response = session.post(url, headers=headers, data=data)
    if response.status_code != 200:
        logger.error(f"Failed to fetch access token: {response.status_code} - {response.text}")
        raise ValueError(f"Failed to fetch access token: {response.status_code} - {response.text}")
    
    jsr = response.json()
    output_dir = os.path.join(os.path.expanduser("~"), "Documents", "TradingBotLogs")
    os.makedirs(output_dir, exist_ok=True)
    token_file = os.path.join(output_dir, 'accessToken.json')
    with open(token_file, 'w') as file:
        file.write(jsr['access_token'])
    logger.info(f"Access Token: {jsr['access_token']}")
    print(f"Access Token: {jsr['access_token']}")
except Exception as e:
    logger.error(f"Failed to fetch access token: {str(e)}")
    raise