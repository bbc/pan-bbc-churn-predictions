
# Overview

- **Project title:** Pan BBC Churn
- **Date from:** 03-06-2019
- **Date to:** Ongoing 
- **Main Stakeholder department:** Pan BBC
- **Main Stakeholder:** 
- **Author:** Alex Philpotts

[Dropbox Paper](https://paper.dropbox.com/doc/Pan-BBC-Churn--AeZw8tgTJnd9NdDzM~VLphORAQ-ALg3K0vwV7NItTAacf2kC)

# Details
  
#### How was the project initiated?

Discussion internal to the Data Science team around the absence of churn modelling in the BBC.

#### What was the goal of the project

Establishing a scale-able framework for churn modelling across products, with a long-term goal of a pan-BBC churn predictions and forecasting.

#### Outputs

- There is a notebook walkthrough of the model design [here](https://github.com/bbc/pan-bbc-churn/blob/master/churn-train.ipynb).
- There is also a notebook on interpreting the model [here](https://github.com/bbc/pan-bbc-churn/blob/master/churn-fi.ipynb).

#### Next steps

- Extending to scoring new data
- Tuning and optimisation
- Additional logistic regression on PCA reduced data
- Unsupervised segmentation as a weak learner
- Neural Network as a weak learner


#### Insights

For more detailed information see the documents above.

We found the following high-level insights:

- _Pending_

#### Recommendations (if any)

- _Pending_

#### Key challenges (if any)

- _Pending_

#### Changes made by Stakeholder(s) (if any)

- _None_

#### Quotes from stakeholder (for new projects)

- _None_

# Impact & Additional Work

#### Impact of change

- _Pending_

#### Further work

- See [Paper](https://paper.dropbox.com/doc/Pan-BBC-Churn--AeZw8tgTJnd9NdDzM~VLphORAQ-ALg3K0vwV7NItTAacf2kC) for ongoing further work.

#### Could this be applied to other products?

One of the core goals for pan-BBC churn modelling is a scale-able model that can be freely applied to other products with minimal re-engineering.

#### Actioning needed (if any) 

N/A.

# Engineering pipeline runbook

###Main overview

- This data pipeline has been developed to support the data science efforts used towards predicting user churn across BBC products.
- It runs every Monday at 18:00.
- Based on Redshift performance, this pipeline takes up to 7 hours due to the slow processing of SQL queries (explained below)


### Pipeline mechanism

- CI/CD: This pipeline is built and released by Jenkins (our team's choice of continous integration and deployment tool) into our Apache Airflow TEST and LIVE environments. 
Steps:
1. Every build is triggered by a new push to this Github repository.
2.The Jenkins build packages the code accordingly and sends the Airflow DAG and plugins code to an AWS S3 bucket (test-airflow-dag-deployments or live-airflow-dag-deployments)
3. Once the code has been stored in the relevant S3 bucket, an AWS Lambda function is triggered and copies both the DAGs and plugins code to the running Apache Airflow EC2 instances.
4. Lastly, as the code is stored in the relevant EC2 instances, it is picked up by the Apache Airflow instance and run accordingly. 
5. Extra step: This Jenkins build also copies the Python scripts used for training and prediction to relevant S3 locations

###List of steps
This pipeline consists of the following steps: 
- Steps 1-12: A set of SQL queries ran against AWS Redshift. These queries build the training and score set for the future model training steps.
- Step 13: Extracts AWS credentials from the EC2 instance, in order to further send relevant AWS commands.
- Step 14-15: These steps unload data from Redshift to S3 via UNLOAD commands. This is done so that the data can be extracted from S3 in future batch processing steps.
- Step 16: This step consists of an AWS Batch job submission. It sends a trigger to AWS Batch to run a predefined job, in a predefined job queue, running a previously stored Docker image from AWS Elastic Container Registry
    #####Notes:
    - In order to run this step, one has to build the Docker iamge from docker/Dockerfile. This is done via the docker-build shell script
    - Local testing can be done by running docker-run.sh Shell script
    - Once a final version is built, the Docker image can be pushed do AWS ECR via the push_docker_image_to_ECR.sh shell script. Account (INT or LIVE), image name and tag details should be changed accordingly
    - This step will run the run-pan-bbc-churn-predictions.sh script inside the Docker image at scale on AWS Batch
    - For a better understanding of this command, please check the run-pan-bbc-churn-predictions.sh script. 
    - This command copies previously generated data from S3 and two Python scripts used for training and prediction. It then runs the relevant Python scripts and outputs the results back to S3
    - Apart from model training and prediction, these Python scripts also send model artifact and parameters data to MLflow, a tool used for tracking machine learning model performance.
- Step 17-21: These steps copy the Python scripts outputs back to the int/live-insights-pan-bbc-churn-predictions S3 buckets. 
It copies propensity scores, shap values, models, logs and charts. For a better understanding of these artifacts, please refer to Alex Philpotts.
- Step 22: The final step appends propensity scores data stored in S3 to a table in AWS Redshift.

###AWS
All related files supporting this pipeline can be found inside AWS S3 in s3://int-insights-pan-bbc-churn-predictions or s3://live-insights-pan-bbc-churn-predictions 

###MLflow
The iplayer_churn_train.py and iplayer_churn_score.py files contain code connecting to existing MLflow instances supported by our team and submits model parameters and artifact values.
Furthermore, these files also contain code storing machine learning models in binary format ( MLflow related or Pickle format) in AWS S3.

For more information about how to connect to the MLflow instances and check machine learning model performance, please visit https://confluence.dev.bbc.co.uk/display/analytics/MLflow

###Future steps

 - BBC Sounds integration: This project will also contain similar code for predicting user churn for BBC Sounds users. A data pipeline integration with new data science code will be needed
 - Unit testing: Model training and prediction code needs robust testing before being deployed in the LIVE environment
 - MLflow and AWS integration testing: Integration tests ensuring connections with MLflow and AWS components work in the LIVE environment are also required
 



