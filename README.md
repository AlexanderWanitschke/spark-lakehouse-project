# Spark Lakehouse Project
> Author: Alexander Lambrecht

## Description
This is a project for practicing and demonstrating results of a lakehouse on AWS S3 with PySpark on AWS Glue and AWS Athena

This project cannot be run locally. It is a collection of PySpark scripts and screenshots of the results.

## Data

Three raw data sources are needed: 
### 1. Customer Records
This is the data from fulfillment and the STEDI website.

[Data Download URL](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/step_trainer/landing)

AWS S3 Bucket URI - s3://cd0030bucket/customers/

It contains the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

### 2. Step Trainer Records
This is the data from the motion sensor.

[Data Download Link](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/step_trainer/landing)

AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/

It contains the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject

### 3. Accelerometer Records
This is the data from the mobile app.

[Data Download URL](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter/accelerometer/landing)

AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/

It contains the following fields:

- timeStamp
- user
- x
- y
- z


## Project structure

    ├── .gitignore         # top-level .gitignore
    ├- ─README.md          # top-level README
    │
    ├── landing zone       # source code and screenshots of landing zone management
    │
    └── trusted zone       # source code and screenshots of trusted zone mangement 
    │
    └── curated zone       # source code and screenshots of curated zone mangement 
    

