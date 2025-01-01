import logging
import socketio
import os
from dotenv import load_dotenv
from PostForecastSiurenitar import update_dataframe

# Load environment variables from .env file
load_dotenv()

# Get the values from the environment variables
BASE_URL = os.getenv('socketBaseURL')  # e.g., 'https://gss.wscada.net/'
SOCKET_NAMESPACE = os.getenv('SOCKET_NAMESPACE')  # e.g., 'river_test' or 'temperature'

# Set up logging
logging.basicConfig(filename='socket_client.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Create a Socket.IO client
socket_gss = socketio.Client()
@socket_gss.event
def connect():
    logging.info('Socket connected')
    # Emit the client_request message with the SOCKET_NAMESPACE
    socket_gss.emit('client_request', SOCKET_NAMESPACE)

@socket_gss.event
def connect_error(error):
    logging.error(f'Connection error: {error}')

@socket_gss.on(SOCKET_NAMESPACE)
def handle_namespace_event(data):
    logging.info("-----------DATA-----------")
    logging.info(f"Received data: {data}")
    
    # Forward the data to data_handler for processing
    update_dataframe(data)

@socket_gss.event
def disconnect():
    logging.info('Socket disconnected')

# Connect to the server
try:
    socket_gss.connect(BASE_URL)
    logging.info(f'Connected to {BASE_URL}')
except Exception as e:
    logging.error(f'Error connecting to {BASE_URL}: {e}')

socket_gss.wait()
