# HybridRecommendation
Implementation for paper: Recommendation Based on Review Texts and Social Communities: A Hybrid Model

## Overview
In this project, we implement a community regression model to predict user ratings towards bussinesse. The project is based on Spark Scala API. It is a local version of our proposed model, you can run it in a single machine in the Spark Standalone Mode. After downloading the spark dependencies and our processed Yelp data, presiction resuls will be printed by executing the Scala2.jar file. Have a good time!

## Requirement
Softeware requirement: Java 1.8

## Preparation
You can download our processed dataset from: 
https://drive.google.com/open?id=1uFmDlS73DRSzjqX7yL2_N3EO05N6iA7L
The executable jar and dependencies from:
https://drive.google.com/open?id=1M566erL8LHjpDLmL7KkeTfeapRMO9_eQ

## Model Training and Testing
To training our hybrid recommendation model, use java -jar command:

eg.
$ java -jar -Xmx10g Scala2.jar --root_path DataDirectory/ 
--coda_result socialUR20CaGroup200.txt

Here is the param list and introduction:
  
  --root_path
  
  This is the root dir where the processed data are stored. You must set this param at first to init our model. 
  
  --task
  
  If you want to random split the processed data to traing and testing set, set "--task DataSplit", else program will find data in the     
  root/output/Access/ floder by default. 
  
  --model_type
  
  The regression model you want to apply. Default is "LR"(Linear Regression).
  
  --word2vec_num
  
  This is a word2vec param which used to set the dimensionality of the word embedding vector. Default is 10.
  
  --review_num
  
  The review number of users. Default is 20.
  
  --min_count
  
  A word2vec param. The minimal occurance number of words. Default is 5.
  
  --window_num
  
  A word2vec param. Default is 5.
  
  --social_type
  
  If you want to choose the community detection algorithms, please set this param to "--social_type coda" or "--social_type cnm". The de
  fault algorithm is coda.
  
  --cnm_result
  
  The file name of the cnm community detection results. Default is  "Yelp2016UserBusinessStarReview"+reviewNum+"cnm2.txt"
  
  --coda_result
  
  The file name of the coda community detection results. Default is "Review"+reviewNum+"mc50xc200ClusterSkipcmtyvv.in.txt"

