import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.pipeline import Pipeline
from kneed import KneeLocator
import seaborn as sns
import IMD as imd

#FURTHER PREPROCESSING STEPS!!

#importing full datasets table after tidy-up done in tidySupDemResult.py
dfCars = pd.read_csv('Results/SupplyDemand/Cars_max_tidy.csv')

#WILL ONLY DO dfCars_max for CLUSTERING!! Will also only use CSUHighest

#bringing in the IMD data (most limiting dataset, goes from GB to England only)
IMD = imd.load_IMD()

#merging IMD data with required KPIs
df = pd.merge(IMD, dfCars, left_on = 'UTLACode', right_on = 'ONSCode', how = 'inner')

#Adding yes/no attributes for the ORCS data (to make it easier to generate summary stats for access to funding)
df['COMP_yn'] = np.where(df['COMP'] != 0, 1, 0)
df['NEW_yn'] = np.where(df['NEW'] != 0, 1, 0)

#Creating two separate DataFrames - one that holds UTLACode and UTLAName and stores against common id
#Another that stores only the id as an identifier and the "KPIs" going into the clustering algorithm

dfIDs = df[['UTLACode','UTLAName','id']]
dfClust = df[['id','AvgScore','EVpct','EVCS','CSUhighest','LA_pct','COMP','NEW']]

clusterCols = ['AvgScore','EVpct','EVCS','CSUhighest','LA_pct','COMP','NEW']

#dropping "id" column for clustering
data = dfClust[clusterCols].values

#placeholder k
k = 2

#setting up the preprocessor, clusterer and full pipe pipelines
preprocessor = Pipeline(
    [
        ("scaler", MinMaxScaler()),
    ]
)

clusterer = Pipeline(
    [
        (
            "kmeans",
            KMeans(
                n_clusters = k,
                init = "k-means++",
                n_init=100,
                max_iter=500
            ),
        ),
    ]
)

pipe = Pipeline(
    [
        ("preprocessor",preprocessor),
        ("clusterer", clusterer)
    ]
)

sse = []
sscore = []

max_k = 11

for k in range(2,max_k):
    
    pipe["clusterer"]["kmeans"].n_clusters = k

    pipe.fit(data)

    preprocessed_data = pipe["preprocessor"].transform(data)

    predicted_labels = pipe["clusterer"]["kmeans"].labels_

    scoef = silhouette_score(preprocessed_data, predicted_labels)

    sscore.append(scoef)
    sse.append(pipe["clusterer"]["kmeans"].inertia_)

plt.style.use("ggplot")
plt.plot(range(2, 11), sse)
plt.xticks(range(2, 11))
plt.xlabel("Number of Clusters")
plt.ylabel("SSE")
plt.show()

kl = KneeLocator(range(2,11), sse, curve='convex',direction='decreasing')
n_rec = kl.elbow

plt.style.use("ggplot")
plt.plot(range(2, 11), sscore)
plt.xticks(range(2, 11))
plt.xlabel("Number of Clusters")
plt.ylabel("Silhouette Coefficient")
plt.show()

#re-running with optimal number of clusters

#clustering pipeline
pipe["clusterer"]["kmeans"].n_clusters = int(input('Enter optimal number of clusters: ')) #needed int conversion to work

pipe.fit(data)

preprocessed_data = pipe["preprocessor"].transform(data)

predicted_labels = pipe["clusterer"]["kmeans"].labels_

scoef = silhouette_score(preprocessed_data, predicted_labels)

sscore_new = (scoef)
sse_new = pipe["clusterer"]["kmeans"].inertia_

scaledCols = list(dfClust.columns)
scaledCols.pop(0)

#adding predicted labels to original cluster
dfClust['Cluster'] = pd.Series(predicted_labels)
#building dataframe with scaled data
dfClust_scaled = pd.DataFrame(preprocessed_data,columns=scaledCols)
dfClust_scaled['Cluster'] = pd.Series(predicted_labels)
dfClust_scaled['id'] = dfClust['id']

centroids = pipe["clusterer"]["kmeans"].cluster_centers_

inverted_centroids = pipe["preprocessor"]["scaler"].inverse_transform(centroids)

cols = clusterCols

dfCentroids = pd.DataFrame(inverted_centroids,columns=cols)
dfCentroids['Cluster_id'] = dfCentroids.index

dfScaledCentroids = pd.DataFrame(centroids,columns=cols)

cols = ['Cluster_id'] + cols
dfCentroids = dfCentroids[cols]

dfFinalClust = pd.merge(dfClust,dfIDs,left_on = 'id', right_on = 'id', how = 'inner')

dfFinalScaled = pd.merge(dfClust_scaled,dfIDs,left_on = 'id', right_on = 'id', how = 'inner')

counts = dfClust['Cluster'].value_counts()
dfCentroids['Counts'] = counts

#Add commands to save outputs!!
dfCentroids.to_csv('Results/Clustering/Centroids.csv', index = False)
dfFinalClust.to_csv('Results/Clustering/DataWithCluster.csv', index = False)
dfScaledCentroids.to_csv('Results/Clustering/ScaledCentroids.csv', index = False)
dfFinalScaled.to_csv('Results/Clustering/ScaledDataWithCluster.csv', index = False)

#saving sse and silhouette scores
d = {'SSE':sse,'Sil':sscore}
scores = pd.DataFrame(d)
scores['n'] = pd.Series(np.arange(2,max_k,1))

scores.to_csv('Results/Clustering/scores.csv', index = False)

# can use these below to make dataframe neater
#round = np.round(dfCentroids, decimals = 1)
#round['EVCS'] = np.round(dfCentroids['EVCS'], decimals = 0)
#round[['COMP_yn','NEW_yn']] = np.floor(np.abs(round[['COMP_yn','NEW_yn']]))ß