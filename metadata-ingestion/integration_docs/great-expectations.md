# Data Quality with Great Expectations

This guide helps to setup and configure `DatahubValidationAction` in Great Expectations to send validation run details to Datahub using Datahub's Python Rest emitter.


## Capabilities

`DatahubValidationAction` pushes validation run metadata to Datahub. This includes 
- Timestamp of validation run
- Expectations set on a dataset
- Whether or not expectations were met during a particular validation run

This integration supports v3 api datasources using SqlAlchemyExecutionEngine. 

## Known Limitations

This integration does not support
- RuntimeQueryBatchSpec with SqlAlchemyExecutionEngine and RuntimeDataConnector
- v2 datasources such as SqlAlchemyDataset
- Datasources using execution engine other than SqlAlchemyExecutionEngine

## Seting up 

1. Install the required dependency in your Great Expectations environment.  
    ```shell
    pip install 'acryl-datahub[great-expectations]'
    ```


2. To add `DatahubValidationAction` in Great Expectations Checkpoint, add following configuration in action_list for your Great Expectations `Checkpoint`. For more details on setting action_list, see [Checkpoints and Actions](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions/) 
    ```yml
    action_list:
      - name: datahub_action
        action:
          module_name: datahub.integrations.great_expectations
          class_name: DatahubValidationAction
          server_url: http://localhost:8080 #datahub server url
    ```
    **Configuration options:**
    - `server_url` (required): URL of DataHub GMS endpoint
    - `env` (optional, defaults to "PROD"): Environment to use in namespace when constructing dataset URNs.
    - `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall checkpoint to fail. Note that configuration issues will still throw exceptions.
    - `token` (optional): Bearer token used for authentication.
    - `timeout_sec` (optional): Per-HTTP request timeout.
    - `extra_headers` (optional): Extra headers which will be added to the datahub request.