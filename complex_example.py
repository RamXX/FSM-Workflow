import asyncio
import random
from typing import Optional, Tuple, Any
from dotenv import load_dotenv
from datetime import datetime

from fsmworkflow import (
    DBConfig, WorkflowContext, WorkflowDefinition, WorkflowStep,
    PersistentWorkflowStateMachine, Transition, Condition
)

# Custom condition functions
def is_high_priority(value: Any) -> bool:
    return isinstance(value, str) and value.lower() == 'high'

def is_customer_satisfied(value: Any) -> bool:
    return isinstance(value, int) and value >= 4

# Simulated database of support agents
SUPPORT_AGENTS = ['Alice', 'Bob', 'Charlie', 'Diana']

# Workflow steps
async def create_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print("Creating new support ticket...")
    context.data['ticket_id'] = f"TICKET-{random.randint(1000, 9999)}"
    context.data['creation_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context.data['status'] = 'New'
    context.data['priority'] = random.choice(['Low', 'Medium', 'High'])
    print(f"Ticket {context.data['ticket_id']} created with {context.data['priority']} priority.")
    return context, None

async def triage_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Triaging ticket {context.data['ticket_id']}...")
    context.data['assigned_agent'] = random.choice(SUPPORT_AGENTS)
    context.data['status'] = 'Triaged'
    print(f"Ticket assigned to {context.data['assigned_agent']}.")
    return context, None

async def investigate_issue(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Investigating issue for ticket {context.data['ticket_id']}...")
    context.data['investigation_complete'] = True
    context.data['solution_found'] = random.choice([True, False])
    context.data['status'] = 'Investigated'
    return context, None

async def escalate_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Escalating ticket {context.data['ticket_id']}...")
    context.data['escalation_level'] = context.data.get('escalation_level', 0) + 1
    context.data['status'] = 'Escalated'
    print(f"Ticket escalated to level {context.data['escalation_level']}.")
    return context, None

async def resolve_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Resolving ticket {context.data['ticket_id']}...")
    context.data['resolution_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context.data['status'] = 'Resolved'
    print(f"Ticket resolved by {context.data['assigned_agent']}.")
    return context, None

async def request_customer_feedback(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Requesting customer feedback for ticket {context.data['ticket_id']}...")
    context.data['feedback_requested'] = True
    context.data['customer_satisfaction'] = random.randint(1, 5)  # 1-5 star rating
    print(f"Customer gave a {context.data['customer_satisfaction']}-star rating.")
    return context, None

async def close_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    ticket_id = context.data.get('ticket_id', 'Unknown')
    print(f"Closing ticket {ticket_id}...")
    context.data['closure_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context.data['status'] = 'Closed'
    print(f"Ticket {ticket_id} closed.")
    return context, None

async def reopen_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    print(f"Reopening ticket {context.data['ticket_id']}...")
    context.data['reopen_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context.data['status'] = 'Reopened'
    print(f"Ticket {context.data['ticket_id']} reopened for further investigation.")
    return context, None

async def finalize_ticket(context: WorkflowContext) -> Tuple[WorkflowContext, Optional[str]]:
    ticket_id = context.data.get('ticket_id', 'Unknown')
    print(f"Finalizing ticket {ticket_id}...")
    context.data['finalized'] = True
    print(f"Ticket {ticket_id} has been finalized and archived.")
    return context, None


async def main() -> None:
    load_dotenv()
    db_config = DBConfig()
    
    workflow_definition = WorkflowDefinition(
        name="Support Ticket Management Workflow",
        steps=[
            "create_ticket", "triage_ticket", "investigate_issue", "escalate_ticket",
            "resolve_ticket", "request_customer_feedback", "close_ticket", "reopen_ticket",
            "finalize_ticket"
        ],
        transitions=[
            Transition(trigger="ticket_created", source="create_ticket", dest="triage_ticket"),
            Transition(trigger="high_priority", source="triage_ticket", dest="escalate_ticket",
                       conditions=[Condition(field="priority", operator="custom", value=None, custom_function=is_high_priority)]),
            Transition(trigger="normal_priority", source="triage_ticket", dest="investigate_issue",
                       conditions=[Condition(field="priority", operator="ne", value="High")]),
            Transition(trigger="investigation_complete", source="investigate_issue", dest="resolve_ticket",
                       conditions=[Condition(field="solution_found", operator="eq", value=True)]),
            Transition(trigger="need_escalation", source="investigate_issue", dest="escalate_ticket",
                       conditions=[Condition(field="solution_found", operator="eq", value=False)]),
            Transition(trigger="escalated", source="escalate_ticket", dest="investigate_issue"),
            Transition(trigger="resolved", source="resolve_ticket", dest="request_customer_feedback"),
            Transition(trigger="feedback_received", source="request_customer_feedback", dest="close_ticket",
                       conditions=[Condition(field="customer_satisfaction", operator="custom", value=None, custom_function=is_customer_satisfied)]),
            Transition(trigger="customer_unsatisfied", source="request_customer_feedback", dest="reopen_ticket",
                       conditions=[Condition(field="customer_satisfaction", operator="lt", value=4)]),
            Transition(trigger="reopened", source="reopen_ticket", dest="investigate_issue"),
            Transition(trigger="ticket_closed", source="close_ticket", dest="finalize_ticket"),
            Transition(trigger="finalized", source="finalize_ticket", dest=None)
        ]
    )

    workflow = PersistentWorkflowStateMachine(db_config, workflow_definition)

    workflow.steps = {
        "create_ticket": WorkflowStep(name="create_ticket", execute=create_ticket),
        "triage_ticket": WorkflowStep(name="triage_ticket", execute=triage_ticket),
        "investigate_issue": WorkflowStep(name="investigate_issue", execute=investigate_issue),
        "escalate_ticket": WorkflowStep(name="escalate_ticket", execute=escalate_ticket),
        "resolve_ticket": WorkflowStep(name="resolve_ticket", execute=resolve_ticket),
        "request_customer_feedback": WorkflowStep(name="request_customer_feedback", execute=request_customer_feedback),
        "close_ticket": WorkflowStep(name="close_ticket", execute=close_ticket),
        "reopen_ticket": WorkflowStep(name="reopen_ticket", execute=reopen_ticket),
        "finalize_ticket": WorkflowStep(name="finalize_ticket", execute=finalize_ticket),
    }

    await workflow.run_workflow()

if __name__ == "__main__":
    asyncio.run(main())