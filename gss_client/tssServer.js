import { fetchObsDataSeries } from './apiService.js'; 
import express from 'express';
const app = express();
const PORT = 3000; 


app.get('/fetch-data',async(req,res)=>{
    const { seriesId, start, end } = req.query;
    const data = await fetchObsDataSeries(seriesId, start, end);
    res.json(data);
})

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));