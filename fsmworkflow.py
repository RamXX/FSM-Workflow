import asyncpg
import uuid
import json
import re
import inspect
import asyncio

from typing import Dict, Any, Optional, List, Tuple, Callable, Union
from typing_extensions import Coroutine
from transitions.extensions.asyncio import AsyncMachine
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

class DBConfig(BaseSettings):
    database: Optional[str] = None
    name: Optional[str] = None
    user: str
    password: str
    host: str
    port: int = 5432

    class Config:
        env_prefix = "DB_"
        extra = "allow"

    def __init__(self, **data):
        super().__init__(**data)
        
        if self.database is None:
            self.database = self.name or "fsmdb"
        
        self.pool: Optional[asyncpg.Pool] = None

    def dict(self, *args, **kwargs):
        d = self.model_dump() if hasattr(self, 'model_dump') else super().dict(*args, **kwargs)
        for key in ['name', 'pool']:
            d.pop(key, None)
        return d

    async def setup(self) -> None:
        """
        Set up the database connection pool.
        """
        if self.pool is None:
            self.pool = await asyncpg.create_pool(**self.dict())

    async def init_db(self) -> None:
        """
        Initialize the database table for workflow states.
        
        This method is idempotent and can be safely called multiple times.
        It will create the table if it doesn't exist and ensure it has the correct schema.
        
        Raises:
            asyncpg.exceptions.PostgresError: If there's an issue with the database connection or permissions.
            RuntimeError: If unable to establish a database connection.
            Exception: For any other unexpected errors during the process.
        """
        try:
            await self.setup()
            
            if self.pool is None:
                raise RuntimeError("Failed to establish database connection. Please check your database configuration.")
            
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS workflow_states (
                        workflow_id UUID PRIMARY KEY,
                        workflow_name TEXT NOT NULL,
                        current_state TEXT NOT NULL,
                        context JSONB NOT NULL,
                        completed BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                columns_to_check = [
                    ('workflow_id', 'UUID'),
                    ('workflow_name', 'TEXT'),
                    ('current_state', 'TEXT'),
                    ('context', 'JSONB'),
                    ('completed', 'BOOLEAN'),
                    ('created_at', 'TIMESTAMP WITH TIME ZONE'),
                    ('updated_at', 'TIMESTAMP WITH TIME ZONE')
                ]
                
                for column_name, column_type in columns_to_check:
                    await conn.execute(f"""
                        DO $$ 
                        BEGIN 
                            IF NOT EXISTS (
                                SELECT 1 
                                FROM information_schema.columns 
                                WHERE table_name='workflow_states' AND column_name='{column_name}'
                            ) THEN 
                                ALTER TABLE workflow_states ADD COLUMN {column_name} {column_type};
                            END IF;
                        END $$;
                    """)
                
                # Speed matters here, so we create an index on workflow_name for faster queries
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_workflow_states_workflow_name 
                    ON workflow_states (workflow_name)
                """)
            
            print("Database table 'workflow_states' initialized successfully.")
        except asyncpg.exceptions.PostgresError as e:
            error_message = f"Database error occurred: {str(e)}"
            if 'permission denied' in str(e).lower():
                error_message += "\nThis may be due to insufficient permissions. Please check your database credentials and ensure the user has the necessary privileges."
            elif 'authentication failed' in str(e).lower():
                error_message += "\nAuthentication failed. Please check your database credentials."
            raise asyncpg.exceptions.PostgresError(error_message) from e
        except RuntimeError as e:
            raise RuntimeError(f"Connection error: {str(e)}") from e
        except Exception as e:
            raise Exception(f"An unexpected error occurred while initializing the database: {str(e)}") from e

    async def reset_db(self) -> None:
        """
        Reset all workflow states in the database.
        
        This method clears all entries from the workflow_states table.
        
        Raises:
            RuntimeError: If unable to establish a database connection.
            asyncpg.exceptions.PostgresError: If there's an issue with the database operation.
            Exception: For any other unexpected errors during the process.
        """
        try:
            await self.setup()
            
            if self.pool is None:
                raise RuntimeError("Failed to establish database connection. Please check your database configuration.")
            
            async with self.pool.acquire() as conn:
                await conn.execute("TRUNCATE TABLE workflow_states;")
            print("Workflow states table has been reset.")
        except asyncpg.exceptions.PostgresError as e:
            raise asyncpg.exceptions.PostgresError(f"Database error occurred while resetting: {str(e)}") from e
        except RuntimeError as e:
            raise RuntimeError(f"Connection error: {str(e)}") from e
        except Exception as e:
            raise Exception(f"An unexpected error occurred while resetting the database: {str(e)}") from e

    async def close(self) -> None:
        """
        Close the database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None

class WorkflowContext(BaseModel):
    """
    Represents the context of a workflow, storing arbitrary data.
    
    Attributes:
        data (Dict[str, Any]): A dictionary to store workflow-specific data.
    """
    data: Dict[str, Any] = Field(default_factory=dict)

class WorkflowState(BaseModel):
    """
    Represents the current state of a workflow.
    
    Attributes:
        workflow_id (str): Unique identifier for the workflow instance.
        workflow_name (str): Name of the workflow.
        current_state (str): The current state of the workflow.
        context (WorkflowContext): The context data for the workflow.
        completed (bool): Indicates if the workflow is completed (default is False).
    """
    workflow_id: str
    workflow_name: str
    current_state: str
    context: WorkflowContext
    completed: bool = False

class Condition(BaseModel):
    """
    Represents a condition for workflow transitions.
    
    Attributes:
        field (str): The field in the context to evaluate.
        operator (str): The operation to perform (e.g., "eq", "gt", "contains").
        value (Any): The value to compare against.
        custom_function (Optional[Callable[[Any], Union[bool, Coroutine[Any, Any, bool]]]]): 
            A custom function for complex conditions. Can be sync or async.
    """
    field: str
    operator: str
    value: Any
    custom_function: Optional[Callable[[Any], Union[bool, Coroutine[Any, Any, bool]]]] = None

    class Config:
        arbitrary_types_allowed = True

class Transition(BaseModel):
    """
    Defines a transition between workflow states.
    
    Attributes:
        trigger (str): The event that triggers this transition.
        source (str | List[str]): The source state(s) for this transition.
        dest (Optional[str]): The destination state after the transition.
        conditions (Optional[List[Condition]]): Conditions that must be met for the transition.
        after (Optional[str]): Action to perform after the transition.
    """
    trigger: str
    source: str | List[str]
    dest: Optional[str]
    conditions: Optional[List[Condition]] = None
    after: Optional[str] = None

class WorkflowDefinition(BaseModel):
    """
    Defines the structure of a workflow.
    
    Attributes:
        name (str): The name of the workflow.
        steps (List[str]): The ordered list of steps in the workflow.
        transitions (List[Transition]): The transitions between workflow states.
    """
    name: str
    steps: List[str]
    transitions: List[Transition]

class WorkflowStep(BaseModel):
    """
    Represents a single step in a workflow.
    
    Attributes:
        name (str): The name of the step.
        execute (Callable): An async function to execute for this step.
    """
    name: str
    execute: Callable[[WorkflowContext], Coroutine[Any, Any, Tuple[WorkflowContext, Optional[str]]]]

class PersistentWorkflowStateMachine(AsyncMachine):
    def __init__(self, db_config: DBConfig, workflow_definition: WorkflowDefinition):
        self.db_config = db_config
        self.workflow_definition = workflow_definition
        self.workflow_state: Optional[WorkflowState] = None
        self.steps: Dict[str, WorkflowStep] = {}

        states = workflow_definition.steps
        transitions = [
            {
                'trigger': t.trigger,
                'source': t.source,
                'dest': t.dest,
                'conditions': self.create_condition_checker(t.conditions) if t.conditions else None,
                'after': t.after
            }
            for t in workflow_definition.transitions
        ]

        super().__init__(
            states=states,
            transitions=transitions,
            initial=states[0],
            send_event=True
        )

    async def setup(self) -> None:
        """
        Set up the database connection pool.
        """
        await self.db_config.setup()

    def create_condition_checker(self, conditions: List[Condition]):
        """
        Create a function to check all conditions for a transition.
        
        Args:
            conditions (List[Condition]): List of conditions to check.
        
        Returns:
            Callable: A function that evaluates all conditions.
        """
        async def check_conditions(event_data):
            results = await asyncio.gather(*(self.evaluate_condition(condition) for condition in conditions))
            return all(results)
        return check_conditions

    async def evaluate_condition(self, condition: Condition) -> bool:
        """
        Evaluate a single condition against the current workflow context.
        
        Args:
            condition (Condition): The condition to evaluate.
        
        Returns:
            bool: True if the condition is met, False otherwise.
        """
        if self.workflow_state is None:
            return False
        
        field_value = self.workflow_state.context.data.get(condition.field)
        
        if condition.custom_function:
            if inspect.iscoroutinefunction(condition.custom_function):
                return await condition.custom_function(field_value)
            else:
                return bool(condition.custom_function(field_value))

        if field_value is None:
            if condition.operator in ["is_none", "is_not_none"]:
                return self._evaluate_none_conditions(condition.operator)
            return False

        if isinstance(field_value, (int, float)) and isinstance(condition.value, (int, float)):
            return self._evaluate_numeric_conditions(field_value, condition.operator, condition.value)
        
        elif isinstance(field_value, str) and isinstance(condition.value, str):
            return self._evaluate_string_conditions(field_value, condition.operator, condition.value)
        
        elif isinstance(field_value, bool) and isinstance(condition.value, bool):
            return self._evaluate_boolean_conditions(field_value, condition.operator, condition.value)
        
        elif isinstance(field_value, list):
            return self._evaluate_list_conditions(field_value, condition.operator, condition.value)
        
        return False

    def _evaluate_none_conditions(self, operator: str) -> bool:
        if operator == "is_none":
            return True
        elif operator == "is_not_none":
            return False
        return False

    def _evaluate_numeric_conditions(self, field_value: Union[int, float], operator: str, condition_value: Union[int, float]) -> bool:
        if operator == "eq":
            return field_value == condition_value
        elif operator == "ne":
            return field_value != condition_value
        elif operator == "gt":
            return field_value > condition_value
        elif operator == "ge":
            return field_value >= condition_value
        elif operator == "lt":
            return field_value < condition_value
        elif operator == "le":
            return field_value <= condition_value
        return False

    def _evaluate_string_conditions(self, field_value: str, operator: str, condition_value: str) -> bool:
        if operator == "eq":
            return field_value == condition_value
        elif operator == "ne":
            return field_value != condition_value
        elif operator == "contains":
            return condition_value in field_value
        elif operator == "not_contains":
            return condition_value not in field_value
        elif operator == "starts_with":
            return field_value.startswith(condition_value)
        elif operator == "ends_with":
            return field_value.endswith(condition_value)
        elif operator == "matches":
            return bool(re.match(condition_value, field_value))
        return False

    def _evaluate_boolean_conditions(self, field_value: bool, operator: str, condition_value: bool) -> bool:
        if operator == "eq":
            return field_value == condition_value
        elif operator == "ne":
            return field_value != condition_value
        return False

    def _evaluate_list_conditions(self, field_value: list, operator: str, condition_value: Any) -> bool:
        if operator == "contains":
            return condition_value in field_value
        elif operator == "not_contains":
            return condition_value not in field_value
        elif operator == "is_empty":
            return len(field_value) == 0
        elif operator == "is_not_empty":
            return len(field_value) > 0
        return False

    async def load_or_create_workflow(self) -> None:
        """
        Load an existing workflow from the database or create a new one if it doesn't exist.
        
        This method initializes the workflow_state attribute.
        
        Raises:
            RuntimeError: If the database pool is not initialized.
        """
        if self.db_config.pool is None:
            raise RuntimeError("Database pool is not initialized. Call setup() first.")
        
        async with self.db_config.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT workflow_id, current_state, context, completed
                FROM workflow_states
                WHERE workflow_name = $1 AND completed = false
                ORDER BY workflow_id DESC
                LIMIT 1
            """, self.workflow_definition.name)
            
            if row:
                self.workflow_state = WorkflowState(
                    workflow_id=row['workflow_id'],
                    workflow_name=self.workflow_definition.name,
                    current_state=row['current_state'],
                    context=WorkflowContext(data=json.loads(row['context'])),
                    completed=row['completed']
                )
                self.set_state(self.workflow_state.current_state)
                print(f"Resuming workflow {self.workflow_state.workflow_id} from state: {self.state}")
            else:
                new_workflow_id = str(uuid.uuid4())
                self.workflow_state = WorkflowState(
                    workflow_id=new_workflow_id,
                    workflow_name=self.workflow_definition.name,
                    current_state=self.workflow_definition.steps[0],
                    context=WorkflowContext()
                )
                self.set_state(self.workflow_definition.steps[0])
                print(f"Starting new workflow with ID: {new_workflow_id}")

    async def save_state(self) -> None:
        """
        Save the current workflow state to the database.
        
        Raises:
            RuntimeError: If the workflow state or database pool is not initialized.
        """
        if self.workflow_state is None:
            raise RuntimeError("Workflow state is not initialized.")
        
        if self.db_config.pool is None:
            raise RuntimeError("Database pool is not initialized.")
        
        async with self.db_config.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO workflow_states (workflow_id, workflow_name, current_state, context, completed)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (workflow_id) DO UPDATE
                SET current_state = EXCLUDED.current_state, 
                    context = EXCLUDED.context,
                    completed = EXCLUDED.completed
            """, self.workflow_state.workflow_id, self.workflow_state.workflow_name,
            self.workflow_state.current_state, json.dumps(self.workflow_state.context.dict()),
            self.workflow_state.completed)

    async def run_step(self) -> None:
        """
        Execute the current step of the workflow.
        
        This method runs the current step, updates the workflow state, and triggers the next state if applicable.
        
        Raises:
            RuntimeError: If the workflow state is not initialized or if the current step is not defined.
        """
        if self.workflow_state is None:
            raise RuntimeError("Workflow state is not initialized.")
        
        if self.workflow_state.completed:
            print("Workflow has already been completed.")
            return

        current_step = self.steps.get(self.state)
        if current_step is None:
            raise RuntimeError(f"Step {self.state} is not defined.")

        try:
            print(f"Executing step: {self.state}")
            context, next_state = await current_step.execute(self.workflow_state.context)
            self.workflow_state.context = context
            self.workflow_state.current_state = self.state

            if next_state:
                await self.trigger_next_state(next_state)
            elif self.state == self.workflow_definition.steps[-1]:
                self.workflow_state.completed = True
                print("Workflow completed successfully.")
            else:
                transition_found = await self.trigger_next_state(None)
                if not transition_found:
                    print(f"No valid transition found from {self.state}. Completing workflow.")
                    self.workflow_state.completed = True

            await self.save_state()

        except KeyError as e:
            print(f"Warning: Missing data in context: {str(e)}")
            self.workflow_state.current_state = self.state
            await self.save_state()
        except Exception as e:
            print(f"Error occurred in state {self.state}: {str(e)}")
            self.workflow_state.current_state = self.state
            await self.save_state()
            raise

    async def trigger_next_state(self, next_state: Optional[str]) -> bool:
        """
        Trigger the transition to the next state.
        
        Args:
            next_state (Optional[str]): The next state to transition to. If None, it will attempt to find a valid transition.
        
        Returns:
            bool: True if a transition was triggered, False otherwise.
        """
        if next_state:
            await self.trigger(next_state)
            return True
        else:
            for transition in self.workflow_definition.transitions:
                if isinstance(transition.source, str):
                    sources = [transition.source]
                else:
                    sources = transition.source
                
                if self.state in sources:
                    conditions = transition.conditions or []
                    results = await asyncio.gather(*(self.evaluate_condition(condition) for condition in conditions))
                    if all(results):
                        await self.trigger(transition.trigger)
                        return True
            
            return False

    async def run_workflow(self) -> None:
        """
        Run the entire workflow from start to finish or resume from the last saved state.
        
        This method sets up the database connection, loads or creates a workflow,
        and runs each step until the workflow is completed or an error occurs.
        """
        await self.setup()
        await self.load_or_create_workflow()
        
        try:
            while self.workflow_state is not None and not self.workflow_state.completed:
                await self.run_step()
            
            if self.workflow_state and self.workflow_state.completed:
                print("Workflow completed successfully.")
            else:
                print("Workflow did not complete successfully.")
        finally:
            if self.db_config.pool:
                await self.db_config.pool.close()

    async def mark_completed(self, event_data):
        """
        Mark the current workflow as completed.
        
        Args:
            event_data: Event data passed by the state machine (not used in this method).
        """
        if self.workflow_state:
            self.workflow_state.completed = True