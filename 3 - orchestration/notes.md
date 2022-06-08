## Prefect UI:

We can open the Prefect UI (In Orion) using `prefect orion` to spin up a localhost instance.
If you are using local Orion, you can start the server with:

```bash
prefect orion start
```

## Deployment of Prefect flow:

A deployment is a server-side concept that encapsulates a flow, allowing it to be scheduled and triggered via API.

- Deployments are uniquely identified by the combination of flow_name/deployment_name.
- The deployment stores metadata about where your flow's code is stored and how your flow should be run.

#### Storage:

- We have to define a storage where our flows are saved
- Check the storage with ``prefect storage ls``
- Create a new storage: ``prefect storage create``


#### Add a deployment to the training script

```python
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.flow_runner import SubprocessFlowRunner
from datetime import timedelta
```

```python
  # define deployment spec
DeploymentSpec(
  flow=main,
  name="model_training",
  schedule=IntervalSchedule(interval=timedelta(minutes=5)), #here in practice we would put 1 day, 1 week,...
  flow_runner=SubprocessFlowRunner(),
  tags=["ml"]
)
```

To create a deployment:
`prefect deployment create prefect_deploy.py` 

This only creates the deployment and schedules the runs. It does not know how to run them. To run them, we use Work Queues.

#### Work Queues:

- You create a work queue on the server. Work queues collect scheduled runs for deployments that match their filter criteria.
- You run an agent in the execution environment. Agents poll a specific work queue for new work, take scheduled work from the server, and deploy it for execution.
To run orchestrated deployments, you must configure at least one work queue and agent.

To configure a work queue and agent for orchestrated deployments:
- [Create a work queue](https://orion-docs.prefect.io/concepts/work-queues/#work-queue-configuration)
- [Start an agent](https://orion-docs.prefect.io/concepts/work-queues/#agent-configuration)


- To see all the scheduled runs:
``prefect work-queue preview <UUID>`` .
- To add an agent that will be attached to the work-queue: `prefect agent start <UUID>`.
