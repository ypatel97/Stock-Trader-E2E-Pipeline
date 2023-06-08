# Stock-Trader-E2E-Pipeline

Work In Progress...

## Overview
This project is designed for me to learn to use ML and data engineering tools. I am creating an end to end data pipeline where my project analyzes stock data every week and predicts the stock price.
The RMSE will be recorded, along with the actual stock price of VOO. The models were trained on a local machine and relevant files are transfered to an EC2 instance using Docker.
The EC2 will constantly query new data and run them through the three different ML models (neural network model, gradient boosting model, and a random forest model). The data will then be stored in an S3 bucket, 
and it will be subsequently moved to a Snowflake data warehouse. Airflow will be used to orchestrate this process. Results will be visualized using Tableau. 

## Changes
Since Snowflake is expensive and requires a fee, I will try to accomplish my tasks within the free trial period. Otherwise, I might cut Snowflake out and just directly analyze data from the S3 bucket. 
Another option to Tableau is Grafana, but I haven't researched any benefits of using one over the other. Again, this project is just a means for me to learn the tools of the trade. 
