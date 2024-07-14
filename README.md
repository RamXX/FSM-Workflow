# FSM-Workflow
### A lightweight, async-friendly workflow system with state persistence for Python

FSM-Workflow is an extremely lightweight, async-friendly workflow system with state persistence to PostgreSQL. It's designed to handle complex, multi-step processes that may need to be paused, resumed, or recovered in case of failure.

When the program crashes at any given state, the next time the run happens, the program starts at the beginning of the last unfinished state.

There are many open source workflow engines, but most if not all, depend on either a server component, a cloud service, or are just heavy, making them unsuitable for small programs that require such functionality.

Furthermore, many of them are language-agnostic, which sounds good in principle, but in the end, native Python integration is subject to many limitations.

Modern flow engineering require the consistent definition of flows, steps, and conditional logic that determines the progression through the flow. Async support is ideal when multiple non-blocking flows need to be ran at concurrently.

Finite-state Machines, like the one provided by the `transitions` library (used in this project) are excellent for that purpose, but their state is kept in memory, so when a program crashes and is later resumed, the entire flow needs to start from scratch.

FSM-Workflow solves this by using a simple PostgresSQL table to store the state of every transition when completed, so resumption happens from the last unfinished state.

## Who should use this?

The sweet spot for this system is in single programs that do not need a full-fledge external workflow engine, and yet, enough declarative programmatic controls to allow for a clear definition of states.

It's useful where the workflow steps are discrete, potentially long-running, and where the ability to recover from failures is required, all while also having a commercial-friendly license (MIT).

It was designed to be used in a system where many of the steps include expensive [DSPy](https://github.com/stanfordnlp/dspy) AI pipelines, so being able to recover from a failure and not having to restart the entire flow is key to avoid new calls to LLMs.

## Features

- Asynchronous execution using `asyncio`
- Persistent state storage in PostgreSQL
- Flexible workflow definition using states and transitions
- Conditional transitions based on workflow context
- Easy integration with existing Python projects
- Resumable workflows in case of interruption or failure
- Strongly-typed and documented.

## Limitations

- Hard dependency on PostgresSQL. This would not be hard to change since the usage is relatively simple, but it wasn't in scope for its initial use.
- It only supports `async` functions, although there are some workarounds for non-async functions (see below).
- If a crash occurs, the program doesn't restart on its own. Rather, it relies on an external process or manual control. See below for instructions on how to implement this in different systems.
- Not aware of other servers, or external execution environments.
- Only **State** is saved in the DB. The user is responsible to implement their own checkpoints and save/restore mechanisms for the **Data** in each state.
- No external monitoring tools.
- More than one workflow can be running against the same DB/table but their workflow name **must** be different to avoid picking states not defined in the current workflow in case of a failure.
- No test coverage, entry in pypi, or CI/CD yet. We can consider that depending on interest and adoption.

## Requirements

- Python 3.11+
- PostgresSQL (v16 was used but it should work well with other versions)

## Installation

(Virtual environments are always recommended).

```bash
pip install -r requirements.txt -U
```

Create or modify a `.env` file in your home directory, and make sure it includes the following variables, changing the values as you see fit:

```bash
DB_HOST="127.0.0.1"
DB_PORT="5432"
DB_NAME="mydb"
DB_USER="myuser"
DB_PASSWORD="myuserpassword"
```


## Usage

### 1. First-time setup

Create the Postgres table by calling the `init_db()` method in the DBConfig class. Here is a basic example:

```python
from fsmworkflow import DBConfig

db_config = DBConfig()
await db_config.init_db()
await db_config.close()
```

Or, if you already have the table and you just want to reset it by cleaning up all its content, you can call `db_config.reset_db()` instead of `init_db()`.

If you get an error in this stage, make sure your Postgres credentials have enough permissions to perfomr the table and index creation.

### 2. Define your workflow

We include two example programs that can give you a good idea how to use this: 

- `basic_example.py` - defines the simplest workflow and is a good skeleton to get started.
- `complex_example.py` - Provides a more comprehensive example that tries to leverage as many features as possible.

Use them as a starting point.


### 3. Create and run the workflow

```python
import asyncio
from workflow import PersistentWorkflowStateMachine

async def main():
    workflow = PersistentWorkflowStateMachine(db_config, workflow_definition)
    workflow.steps = steps
    await workflow.run_workflow()

asyncio.run(main())
```

## Advanced Usage

### Custom Conditions

You can define custom conditions for transitions by creating any function that returns a `bool`:

```python
def custom_condition(value: Any) -> bool:
    # Your custom logic here
    return value > 10

condition = Condition(field="some_field", operator="custom", value=None, custom_function=custom_condition)
```
The function can be either sync or async.

### Error Handling

The workflow system automatically saves the state after each step. If an error occurs, you can resume the workflow from the last successful state:

```python
try:
    await workflow.run_workflow()
except Exception as e:
    print(f"An error occurred: {str(e)}")
    print("You can resume the workflow later from the last saved state.")
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.