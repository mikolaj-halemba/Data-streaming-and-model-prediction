
<h1 align="center">Kafka-Streaming-and-Model-Prediction</h1>

<p align="center"> This project consists of three parts: streaming data into Kafka from a CSV file, applying a machine learning model to make predictions on the streamed data using Spark, writing the data to a MongoDB database and creating visualisations for the streamed data using Python.</p>


## Screenshots
![kafka-streaming](https://github.com/mikolaj-halemba/Kafka-streaming-and-model-prediction/blob/main/images/image_1.png)
![kafka-streaming](https://github.com/mikolaj-halemba/Kafka-streaming-and-model-prediction/blob/main/images/image_2.png)

## Built With
- PySpark
- Kafka
- Python
- Docker-compose

### Requirements
- Pandas
- Joblib
- pymongo
- confluent_kafka
- dateutil
- argparse
- json
- socket

<h2> Instructions: </h2>
- Navigate to the folder containing the docker-compose.yml file and launch it with the command: "docker compose up". </br>
- Create a new topic by running the following command: "docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic appML". </br>
- Access the Jupyter Lab container; the password is "root". </br>
- Open a terminal in Jupyter Lab and initiate the data stream by executing the command: "python sendStream.py data_to_send.csv appML". </br>
- Open another terminal in Jupyter Lab and initiate the data collection process by executing the command: "spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 app.py". </br>
- Launch the "Visualizations.ipynb" notebook and enjoy the view of your streaming data. </br>
Remember to ensure that the Kafka server and MongoDB server are running properly before proceeding with the steps above.

<h2>Structure: </h2>
- app.py: Script for reading data from Kafka, applying the schema, using the trained model for prediction and storing the results in MongoDB.
- sendStream.py: Script for streaming data from a CSV file to a Kafka topic.
- clf_model.pkl: Trained machine learning model.
- config.json: Configuration file for MongoDB connection.

## Author

**Halemba Mikołaj**


- [Profile](https://github.com/mikolaj-halemba "Halemba Mikołaj")
- [Email](mailto:mikolaj.halemba96@gmail.com?subject=Hi "Hi!")


## 🤝 Support

Contributions, issues, and feature requests are welcome!

Give a ⭐️ if you like this project!


