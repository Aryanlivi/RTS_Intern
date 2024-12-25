const axios = require('axios');
require('dotenv').config(); 


// Base configuration
const axiosInstance = axios.create({
    baseURL: process.env.BASE_URL,
});

const SOCKET_NAMESPACE = process.env.SOCKET_NAMESPACE;
if (!process.env.BASE_URL || !process.env.SOCKET_NAMESPACE) {
    throw new Error("Environment variables BASE_URL and SOCKET_NAMESPACE are required.");
}

// Endpoints
const ENDPOINTS = {
    socketResponse: `api/socket/${SOCKET_NAMESPACE}/response`,
    station: "api/station/",
    stationById:(stationId)=>`/api/station/${stationId}/`,
    stationDataSeries: (stationId) => `/api/station/${stationId}/data-series/`,
    //Date format: YYYY-MM-DDTHH:mm:ss
    observationDataSeries:(seriesId,startDate,endDate)=> `api/observation?series_id=${seriesId}&date_from=${startDate}&date_to=${endDate}`,
    observationDataSeriesLatest:(seriesId)=>`api/observation/${seriesId}/latest`,
};

const fetchData=async()=>{
    try{
        const response=await axiosInstance.get(ENDPOINTS.socketResponse)
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

const fetchAllStation=async()=>{
    try{
        console.log("Retrieving All Station data(id, name, latitude, longitude etc)..")
        const response=await axiosInstance.get(ENDPOINTS.station)
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

const fetchStationById=async(stationId)=>{
    try{
        console.log(`Retrieving Station data for id=${stationId}...`)
        const response=await axiosInstance.get(ENDPOINTS.stationById(stationId))
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

////----------Station Data Series-------------///
const fetchStationDataSeries=async(stationId)=>{
    try{
        console.log(`Retrieving list of data series of a particular station with id=${stationId}...`)
        const response=await axiosInstance.get(ENDPOINTS.stationDataSeries(stationId))
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

////-------Obsercation -----------//

const fetchObsDataSeries=async(seriesId,startDate,endDate)=>{
    try{
        console.log(`Fetching point observation of data series with series_id=${seriesId} from ${startDate} to ${endDate}`)
        const response=await axiosInstance.get(ENDPOINTS.observationDataSeries(seriesId,startDate,endDate))
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

const fetchObsDataSeriesLatest= async(seriesId)=>{
    try{
        console.log(`Fetching Latest point observation of data series with series_id=${seriesId}`)
        const response=await axiosInstance.get(ENDPOINTS.observationDataSeriesLatest(seriesId))
        console.log(response.data)
    }
    catch(error){
        console.error('Error fetching data:', error.message);
    }
}

// fetchData()

// fetchStationById(244)
// fetchStationDataSeries(244)

waterlevelSeriesId=6868
// fetchObsDataSeries(waterlevelSeriesId,'2024-12-10T00:00:00','2024-12-12T00:00:00')
fetchObsDataSeriesLatest(waterlevelSeriesId)