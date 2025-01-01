import socketio# NEED Socket V2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the values from the environment variables
BASE_URL = os.getenv('socketBaseURL')  # e.g., 'https://gss.wscada.net/'
SOCKET_NAMESPACE = os.getenv('SOCKET_NAMESPACE')  # e.g., 'river_test' or 'temperature'
print(f"BASE_URL: {BASE_URL}")
print(f"SOCKET_NAMESPACE: {SOCKET_NAMESPACE}")

# Create a Socket.IO client
socket_gss = socketio.Client()

# Define event handlers
@socket_gss.event
def connect():
    print('Socket connected')

@socket_gss.event
def connect_error(error):
    print(f'Connection error: {error}')

@socket_gss.event
def disconnect():
    print('Socket disconnected')

# Connect to the server
try:
    socket_gss.connect(BASE_URL)
    print(f'Connected to {BASE_URL}')
except Exception as e:
    print(f'Error connecting to {BASE_URL}: {e}')

# Keep the connection alive
socket_gss.wait()
