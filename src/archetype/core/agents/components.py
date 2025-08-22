
from archetype.core.component import Component


class ObsComponent(Component):
    agent_id: str
    features_json: str
    tick: int


class ActionProposal(Component):
    agent_id: str
    action_name: str
    params_json: str | None = None
    priority: float = 0.0
    planner_id: str = ""
    tick: int


class ChosenAction(Component):
    agent_id: str
    action_name: str
    params_json: str | None = None
    planner_id: str = ""
    tick: int


class Outcome(Component):
    agent_id: str
    reward: float
    cost: float
    judge_version: str = ""
    tick: int


class BanditArmState(Component):
    agent_id: str
    arm_name: str
    pulls: int = 0
    mean_reward: float = 0.0
    est_cost: float = 0.0
    last_visit_ts: int | None = None

class BanditPolicy(Component):
    agent_id: str
    planner_id: str = "bandit"
    mode: str = "ucb"           # or "linucb", "thompson"
    c_ucb: float | None = None
    lambda_cost: float | None = None
    alpha: float | None = None    # LinUCB only
    seed: int | None = None       # Thompson only


