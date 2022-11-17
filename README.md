# Getting Started

1. Download the datasets from the Microsoft Azure Predictive Maintenance [Kaggle project](https://www.kaggle.com/datasets/arnabbiswas1/microsoft-azure-predictive-maintenance) either directly or by using the Kaggle CLI. Then, upload it to a GCS bucket (in the correct region / project).

```sh 
kaggle datasets download -d arnabbiswas1/microsoft-azure-predictive-maintenance
```

2. Create a virtual environment and pip install requirements.txt locally to ensure you have the necessary versions of the google cloud and kfp libraries installed. 

```sh 
pip install --trusted-host pypip.python.org -r requirements.txt
```

3. Create a [service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console) and copy the resulting json file into the *./data* directory.

4. Fill out the environment variables in **env.sh** and source the file.

```sh 
source env.sh
```

5. Make sure that have setup your [gcloud CLI](https://cloud.google.com/sdk/docs/initializing), including [authorizing your service account](https://cloud.google.com/sdk/gcloud/reference/auth/activate-service-account) and/or [switching to an active account](https://cloud.google.com/sdk/docs/authorizing#switch_the_active_account) that you'd like to use that has the correct privileges / permissions.

6. Run the **build_image.sh** script to build the component container image and push it to the Artifact Registry (make sure your Docker daemon is running).

```sh
. ./build_image.sh
```

7. Run the **run_pipeline.py** script to trigger the end-to-end Vertex AI pipeline (ingests the data into BigQuery from your GCS bucket in Step 1, dbt run, creates and pushes features into Vertex feature store, trains a simple Random Forest classification model via scikit-learn, evaluates the model, and then deploys the model to a Vertex endpoint).

```sh
python run_pipeline.py
```

*NOTE: Pipeline will take approximately 1-2 hours to complete*

8. (Optional) If you would like, run the **cleanup.py** script once you're done and if you don't need the underlying BigQuery dataset, feature store, model, endpoint, or other pipeline assets / resources anymore. 

(Note: You may need to undeploy the model first from the endpoint before being able to delete it)
