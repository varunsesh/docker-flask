# Introduction
This is a basic implementation of a linear regression model using pyspark on predicting housing sales. The data set can be found at https://www.kaggle.com/c/house-prices-advanced-regression-techniques/ . This example displays basic containerisation of model deployment in a docker container. 

# Approach
1. The application uses a flask service to host the web application. The data set is split into training and test data in the ratio 70-30.
1. The model is run using pyspark. First an EDA is performed to understand the data and to deal with spurious data points such as ouliers, null and nan values. When the user requests the various housing prices, the predictions are displayed in a table. The entire application runs in a docker container. 
1. This submission contains a Dockerfile along with the entire application. The user needs to build the image using the docker file and then run the image. This will launch a container where the flask application will be available. Instructions for deployment are provided below. 
1. The flask application runs on port no 5000 inside the container. Make sure that this port is forwarded to localhost. Instructions are given below.
# Deployment instructions
1. The base directory contains the following files:
   1. Dockerfile
   1. The folder "sparkify"

1. To deploy, first build the docker image
`$ docker build -t housing_group1_spark` (you may use any tag you wish)
1. Check that the image is built using:
`$ docker images`
1. Run the container
`$ docker run -p 8000:5000 housing_group1_spark`
1. The app will be available on http:://localhost:8000
