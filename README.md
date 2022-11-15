# Getting Started

1. Download the datasets from the Microsoft Azure Predictive Maintenance [Kaggle project](https://www.kaggle.com/datasets/arnabbiswas1/microsoft-azure-predictive-maintenance) either directly or by using the Kaggle CLI:

```sh 
kaggle datasets download -d arnabbiswas1/microsoft-azure-predictive-maintenance
```

Afterwards, upload it to a GCS bucket (in the correct region).

2. Create a [service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console) and copy the resulting json file into the */data* directory

3. Fill out the environment variables in **env.sh** and run

```sh 
source env.sh
```

4. Run the **build_image.sh** script to build the component container image and push it to the Artifact Registry (make sure your Docker daemon is running).

```sh
. ./build_image.sh
```

5. Run the **run_pipeline.py** script to trigger the end-to-end Vertex AI pipeline (ingests the data into BQ from step 1, dbt run, creates and pushes features into Vertex feature store, trains a simple RF classification model via scikit-learn, evaluates the model, and then finally deploys the model to a Vertex endpoint).

```sh
python run_pipeline.py
```

*NOTE: Pipeline will take approximately 1-2 hours to complete*

6. (Optional) If you would like, run the **cleanup.py** script once you're done and if you don't need the underlying BigQuery dataset, feature store, model, endpoint, and/or other pipeline assets / resources anymore. (Note: You may need to undeploy the model first from the endpoint before being able to delete it)
