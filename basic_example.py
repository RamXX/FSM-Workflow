import asyncio
import random
from typing import Optional, Tuple
from dotenv import load_dotenv

from fsmworkflow import DBConfig, WorkflowContext, WorkflowDefinition, WorkflowStep, PersistentWorkflowStateMachine, Transition, Condition

async def task1(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    context.data['task1_result'] = 'completed'
    return context, None

async def task2(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    result = random.choice(['success', 'failure'])
    context.data['task2_result'] = result
    return context, None

async def task3a(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    context.data['task3_result'] = 'success_path_completed'
    return context, None

async def task3b(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    context.data['task3_result'] = 'failure_path_completed'
    return context, None

async def task4(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    context.data['task4_result'] = 'completed'
    return context, None

async def main() -> None:
    load_dotenv()
    db_config = DBConfig()
    
    workflow_definition = WorkflowDefinition(
        name="Generic Workflow",
        steps=["task1", "task2", "task3a", "task3b", "task4"],
        transitions=[
            Transition(trigger="next", source="task1", dest="task2"),
            Transition(
                trigger="success",
                source="task2",
                dest="task3a",
                conditions=[Condition(field="task2_result", operator="eq", value="success")]
            ),
            Transition(
                trigger="failure",
                source="task2",
                dest="task3b",
                conditions=[Condition(field="task2_result", operator="eq", value="failure")]
            ),
            Transition(trigger="next", source="task3a", dest="task4"),
            Transition(trigger="next", source="task3b", dest="task4"),
            Transition(trigger="complete", source="task4", dest=None),
        ]
    )

    workflow = PersistentWorkflowStateMachine(db_config, workflow_definition)

    workflow.steps = {
        "task1": WorkflowStep(name="task1", execute=task1),
        "task2": WorkflowStep(name="task2", execute=task2),
        "task3a": WorkflowStep(name="task3a", execute=task3a),
        "task3b": WorkflowStep(name="task3b", execute=task3b),
        "task4": WorkflowStep(name="task4", execute=task4),
    }

    try:
        await workflow.run_workflow()
        print("Workflow completed successfully.")
    except Exception as e:
        print(f"An error occurred while running the workflow: {str(e)}")
    finally:
        if db_config.pool:
            await db_config.pool.close()

if __name__ == "__main__":
    asyncio.run(main())