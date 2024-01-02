**Airport Traffic Analysis and Flight Sequence Tracking Using PySpark**


**By: Javad Manashty **



### Description:
This PySpark code is designed for comprehensive airport traffic analysis and tracking the sequence of flights for individual aircraft. The script consists of two primary tasks:

1. **Flight Sequence Tracking**: Determines the sequence of flights based on the turn of each aircraft. For every flight, the code identifies the next flight scheduled for the same aircraft, providing insights into aircraft utilization and scheduling.

2. **Airport Traffic Analysis**: Calculates the number of inbound and outbound flights at an airport in 15-minute intervals within a specified time frame. This analysis helps in understanding the traffic pattern at the airport, facilitating better resource management and operational planning.

The script processes a dataset containing flight details like origin, destination, departure, and arrival times. It uses PySpark's powerful data processing capabilities to handle large volumes of data efficiently.

#### Key Components of the Code:
- **Initialization of Spark Session**: Sets up the environment for data processing.
- **Data Ingestion and Preparation**: Reads a CSV file containing flight data into a Spark DataFrame and prepares the data by converting timestamp strings into actual timestamp data types.
- **String of Flights Construction**: Uses window functions to order data by arrival times for each aircraft and calculates the next flight for each aircraft.
- **Airport Traffic Dataset Creation**: Groups data by 15-minute windows to count inbound and outbound flights, merging these counts to provide a comprehensive view of airport traffic.
- **Output Representation**: The results are displayed in a structured format, showing the sequence of flights for each aircraft and the number of flights arriving and departing from the airport in each time window.

#### Output Verification:
To ensure the correctness and effectiveness of the solution, the script's output is presented alongside the code. The output includes:
- A table showing the sequence of flights for each aircraft, with columns for flight ID, aircraft registration code, actual arrival time, and the next flight ID.
- A detailed table of airport traffic data, listing the number of inbound and outbound flights in each 15-minute interval for specified airports.

This code, along with the provided outputs, serves as proof of the working solution, demonstrating the script's capability to analyze flight data efficiently and effectively using PySpark.

