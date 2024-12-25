const io=require('socket.io-client')
require('dotenv').config(); 


const BASE_URL = process.env.BASE_URL //this is socketV2
const SOCKET_NAMESPACE = process.env.SOCKET_NAMESPACE ; //currently 'river_test' or 'temperature'

// connecting to a Socket.IO server
const socket_gss = io(BASE_URL);

socket_gss.on('connect', () => {
    console.log('Socket connected:', socket_gss.connected);
});
socket_gss.on('connect_error', (err) => {
    console.error('Connection error:', err);
});

socket_gss.emit('client_request', SOCKET_NAMESPACE);
socket_gss.on(SOCKET_NAMESPACE, (response) => {
console.log('Data received from GSS:');
  console.log(response); // Handle the socket response
});


socket_gss.on('disconnect', () => {
    console.log('Socket disconnected');
});

