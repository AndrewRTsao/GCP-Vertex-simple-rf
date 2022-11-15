import os
import yaml
from typing import NamedTuple

from google.cloud import aiplatform
from google.cloud.aiplatform.pipeline_jobs import PipelineJob
from google_cloud_pipeline_components.aiplatform import (EndpointCreateOp, ModelDeployOp)

from kfp import components
from kfp.v2 import dsl
from kfp.v2 import compiler
from kfp.v2.dsl import (Artifact, Dataset, Input, Model, Output,
    Metrics, ClassificationMetrics, component, OutputPath, InputPath)

def run_pipeline():

    # Instantiate env variables  
    input_gcs_path = os.getenv('INPUT_GCS_PATH')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    dataset_name = os.getenv('DATASET_NAME')
    dataset_location = os.getenv('DATASET_LOCATION')
    credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    region = os.getenv('VERTEX_REGION')
    docker_image = os.getenv('VERTEX_DBT_DOCKER_IMAGE')
    prediction_period = os.getenv('PREDICTION_PERIOD')
    
    # Creating bucket and pipeline root where pipeline and model assets will be stored
    bucket_uri = "gs://" + os.getenv('BUCKET_NAME')
    pipeline_root = f"{bucket_uri}/pipeline_root"
    pipeline_root

    # Display name of your pipeline (different from pipeline name)
    pipeline_name = os.getenv('PIPELINE_NAME')
    pipeline_display_name = pipeline_name + "-display_name"

    # Other relevant pipeline variables
    model_display_name = os.getenv('MODEL_DISPLAY_NAME')
    endpoint_name = os.getenv('ENDPOINT_DISPLAY_NAME')
    machine_type = os.getenv('MACHINE_TYPE')
    serving_container_image_uri = os.getenv('SERVING_CONTAINER_IMAGE_URI')

    # Retreive docker image name from build_image.sh and programmatically update component.yaml
    def update_docker_image_in_component_file(
        base_image: str,
        component_file: str = 'component.yaml',
    ):

        # Read and update input component.yaml file with new base image
        with open(component_file) as input:
            comp_file = yaml.safe_load(input)
            comp_file['implementation']['container']['image'] = base_image

        # Persist the changes to a temp yaml file first
        with open("/tmp/temp.yaml", "w") as output:
            yaml.dump(comp_file, output, default_flow_style=False, sort_keys=False)

        # Check that the new yaml file looks correct before renaming and overwriting current component.yaml file
        with open("/tmp/temp.yaml") as check:
            check_comp_file = yaml.safe_load(check)
        
        if check_comp_file['implementation']['container']['image'] == base_image:
            os.rename("/tmp/temp.yaml", component_file)


    # Defining custom component to load training and test data for subsequent model training
    @component(
        base_image = docker_image,
        output_component_file="components/load_training_data_component.yaml",
        packages_to_install=["pandas", "pandas-gbq", "pyarrow", "scikit-learn"],
    )
    def load_training_data(
        sql: str,
        project_id: str,
        credentials_json: str,
        dataset_train: Output[Dataset],
        dataset_test: Output [Dataset],
    ):

        import pandas as pd
        import pandas_gbq
        import numpy as np
        from sklearn.model_selection import train_test_split as tts
        from google.oauth2 import service_account

        # Encoding and dropping string features for RF
        def encode_feature_and_drop(
            df: pd.DataFrame,
            feature: str,
        ):
            dummies = pd.get_dummies(df[[feature]])
            final_df = pd.concat([df, dummies], axis=1)
            final_df = final_df.drop([feature], axis=1)
            return final_df

        credentials = service_account.Credentials.from_service_account_file(credentials_json)
        
        df = pandas_gbq.read_gbq(sql, project_id=project_id, credentials=credentials)
        df = df.drop(['timestamp', 'entity_type_vm_errors', 'entity_type_vm_failures', 'entity_type_vm_hourly_status', 'entity_type_vm_machines', 'entity_type_vm_maintenance', 'entity_type_vm_telemetry'], axis=1)
        df = encode_feature_and_drop(df, "model")

        # Cleaning up NaNs in input dataframe with column medians (for real production - can use more robust imputation methods)
        # Take median for numeric columns when imputing nulls
        for col in df.select_dtypes(include=np.number):
            df[col] = df[col].fillna(df[col].median(), inplace=True)
        
        # Create rolling window of 2 and take max when imputing nulls for booleans
        for col in df.select_dtypes(exclude=np.number):
            df[col] = df[col].fillna(False).rolling(window=2,min_periods=1).max()
 
        # Split training and test
        train, test = tts(df)
        train.to_csv(dataset_train.path + ".csv" , index=False, encoding='utf-8-sig')
        test.to_csv(dataset_test.path + ".csv" , index=False, encoding='utf-8-sig')

    
    # Defining custom model training component (simple RF in this case)
    @component(
        base_image = docker_image,
        output_component_file="components/train_model_component.yaml",
        packages_to_install=["pandas", "scikit-learn"]
    )
    def train_model(
        dataset: Input[Dataset],
        target: str,
        model: Output[Model],
    ):

        from sklearn.ensemble import RandomForestClassifier
        import pandas as pd
        import pickle

        data = pd.read_csv(dataset.path+".csv")
        model_rf = RandomForestClassifier(n_estimators=10)
        model_rf.fit(
            data.drop(columns=[target]),
            data[target],
        )
        model.metadata["framework"] = "RF"
        file_name = model.path + f".pkl"
        with open(file_name, 'wb') as file:  
            pickle.dump(model_rf, file)


    # Defining custom model evaluation component
    @component(
        base_image = docker_image,
        output_component_file="components/evaluate_model_component.yaml",
        packages_to_install=["pandas", "scikit-learn"]
    )
    def evaluate_model(
        test_set:  Input[Dataset],
        target: str,
        rf_model: Input[Model],
        thresholds_dict_str: str,
        metrics: Output[ClassificationMetrics],
        kpi: Output[Metrics]
    ) -> NamedTuple("output", [("deploy", str)]):

        from sklearn.ensemble import RandomForestClassifier
        import pandas as pd
        import logging 
        import pickle
        from sklearn.metrics import roc_curve, confusion_matrix, accuracy_score
        import json
        import typing

        
        def threshold_check(val1, val2):
            cond = "False"
            if val1 >= val2:
                cond = "True"
            return cond

        data = pd.read_csv(test_set.path+".csv")
        model = RandomForestClassifier()
        file_name = rf_model.path + ".pkl"
        with open(file_name, 'rb') as file:  
            model = pickle.load(file)
        
        y_test = data.drop(columns=[target])
        y_target = data[target]
        y_pred = model.predict(y_test)
        
        y_scores =  model.predict_proba(data.drop(columns=[target]))[:, 1]
        fpr, tpr, thresholds = roc_curve(
            y_true=data[target].to_numpy(), y_score=y_scores, pos_label=True
        )
  
        # Log confusion matrix
        metrics.log_confusion_matrix(
            ["False", "True"],
            confusion_matrix(
                data[target], y_pred
            ).tolist(), 
        )
        
        accuracy = accuracy_score(data[target], y_pred.round())
        thresholds_dict = json.loads(thresholds_dict_str)
        rf_model.metadata["accuracy"] = float(accuracy)
        kpi.log_metric("accuracy", float(accuracy))
        deploy = threshold_check(float(accuracy), int(thresholds_dict['roc']))
        return (deploy,)

  
    # Defining custom component to create endpoint and deploy model
    @component(
        base_image = docker_image,
        output_component_file="components/deploy_model_component.yaml",
        packages_to_install=["scikit-learn"]
    )
    def deploy_model(
        model: Input[Model],
        project: str,
        region: str,
        model_display_name: str,
        endpoint_name: str,
        machine_type: str, 
        serving_container_image_uri: str,
        vertex_endpoint: Output[Artifact],
        vertex_model: Output[Model]
    ):
        from google.cloud import aiplatform
        aiplatform.init(project=project, location=region)
        
        def create_endpoint():
            endpoints = aiplatform.Endpoint.list(
            filter='display_name="{}"'.format(endpoint_name),
            order_by='create_time desc',
            project=project, 
            location=region,
            )
            if len(endpoints) > 0:
                endpoint = endpoints[0]  # most recently created
            else:
                endpoint = aiplatform.Endpoint.create(
                display_name=endpoint_name, project=project, location=region
            )
            return endpoint 

        endpoint = create_endpoint()   
        
        # Import a model programmatically
        model_upload = aiplatform.Model.upload(
            display_name = model_display_name, 
            artifact_uri = model.uri.replace("/model", "/"),
            serving_container_image_uri =  serving_container_image_uri,
            serving_container_health_route=f"/v1/models/{model_display_name}",
            serving_container_predict_route=f"/v1/models/{model_display_name}:predict",
            serving_container_environment_variables={
            "MODEL_NAME": model_display_name,
        },       
        )
        model_deploy = model_upload.deploy(
            machine_type=machine_type, 
            endpoint=endpoint,
            traffic_split={"0": 100},
            deployed_model_display_name=model_display_name,
        )

        # Save data to the output params
        vertex_model.uri = model_deploy.resource_name
    
    # Pipeline definition
    @dsl.pipeline(name=pipeline_name, pipeline_root=pipeline_root)
    def pipeline(
        project: str,
        gcp_region: str,
        thresholds_dict_str: str,
        model_display_name: str,
        endpoint_display_name: str,
        machine_type: str = "n1-standard-4",
        serving_container_image_uri: str = "us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest" 
    ):
        
        # Creating component ops for pipeline
        data_ingest_op = data_ingestion_component(input_gcs_path, project_id, dataset_name, dataset_location)
        dbt_op = dbt_component(project_id, dataset_name, credentials)
        feature_store_op = feature_store_component(project_id, dataset_name, region, prediction_period)

        # Retrieving full path to training data and target
        TABLE_PATTERN = "{project}.{dataset}.{table}" 
        training_table = TABLE_PATTERN.format(
            project=project_id, dataset=dataset_name, table="training_data"
        )
        model_target = "failure_in_" + prediction_period
        
        # Construct SQL to retrieve data from BQ table
        sql_query = "SELECT * FROM " + training_table
        
        training_data_op = load_training_data(sql_query, project_id, credentials) 
        train_model_op = train_model(training_data_op.outputs["dataset_train"], target=model_target)
        evaluate_model_op = evaluate_model(
            test_set = training_data_op.outputs["dataset_test"],
            target=model_target,
            rf_model=train_model_op.outputs["model"],
            thresholds_dict_str = thresholds_dict_str, # Only deploy the model if model performance > threshold 
        )

        with dsl.Condition(
            evaluate_model_op.outputs["deploy"]=="True",
            name="deployment-decision",
        ):
            
            deploy_model_op = deploy_model(
                model=train_model_op.outputs['model'],
                project=project,
                region=gcp_region,
                model_display_name=model_display_name,
                endpoint_name=endpoint_display_name,
                machine_type = machine_type,
                serving_container_image_uri = serving_container_image_uri,
            )

        # Specifying order of pipeline components that don't have direct inputs / outputs
        dbt_op.after(data_ingest_op)
        feature_store_op.after(dbt_op)
        training_data_op.after(feature_store_op)


    # Compiles the pipeline defined in the previous function into a json file executable by Vertex AI Pipelines
    def compile():
        compiler.Compiler().compile(
            pipeline_func=pipeline, package_path='vertex_pipeline_simple_rf.json', type_check=False
        )


    # Triggers the pipeline, caching is disabled as this causes successive dbt pipeline steps to be skipped
    def trigger_pipeline():
        pl = PipelineJob(
            display_name=pipeline_display_name,
            enable_caching=False,
            template_path="vertex_pipeline_simple_rf.json",
            pipeline_root=pipeline_root,
            parameter_values={
                "project": project_id,
                "gcp_region": region,
                "thresholds_dict_str": '{"roc": 0.8}',
                "model_display_name": model_display_name,
                "endpoint_display_name": endpoint_name,
                "machine_type": machine_type,
                "serving_container_image_uri": serving_container_image_uri,
            },
        )

        pl.run(sync=True)


    # Initializing client and create feature store
    aiplatform.init(project=project_id, location=region)

    # Setting custom component paths (defined as YAML files)
    script_dir = os.path.dirname(__file__)
    data_ingestion_path = os.path.join(script_dir, 'components/data_ingestion_component.yaml')
    dbt_component_path = os.path.join(script_dir, 'components/dbt_component.yaml')
    feature_store_path = os.path.join(script_dir, 'components/feature_store_component.yaml')

    # Update custom component files with new docker image built from build_image.sh
    update_docker_image_in_component_file(docker_image, data_ingestion_path)
    update_docker_image_in_component_file(docker_image, dbt_component_path)
    update_docker_image_in_component_file(docker_image, feature_store_path)

    # Loads the custom component files as separate components for pipeline (initial data ingestion, dbt run, and feature store creation / serving)
    data_ingestion_component = components.load_component_from_file(data_ingestion_path)
    dbt_component = components.load_component_from_file(dbt_component_path)
    feature_store_component = components.load_component_from_file(feature_store_path)

    # Compile the pipeline components and trigger the run
    compile()
    trigger_pipeline()

if __name__ == '__main__':
    
    run_pipeline()