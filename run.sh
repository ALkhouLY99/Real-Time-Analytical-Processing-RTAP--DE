#!/bin/bash

# Trap Ctrl+C to clean up processes
trap "echo 'Stopping all processes...'; pkill -f python; pkill -f streamlit; exit" SIGINT

# Start main.py and wait 1 second
python main.py &  
sleep 1  

# Start voting_process.py and wait 3 seconds
python voting_process.py &  
sleep 3  

# Start spark-stream.py and wait 3 seconds
python spark-stream.py &  
sleep 3 

# Start Streamlit app in the background
streamlit run app.py &  

# Wait for all background processes
wait  
echo "All processes completed!"
