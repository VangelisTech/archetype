

from daft import DataFrame, col


from archetype.core.aio.async_processor import AsyncProcessor
from .components import ObsComponent, ActionProposal, BanditArmState, BanditPolicy


class BanditSelectProcessor(AsyncProcessor):
    """
    Pure-DataFrame UCB bandit selection per agent.

    Assumptions:
      - Input df contains `ObsComponent` columns (obs__agent_id, obs__tick)
      - Bandit arm state is provided in the same df (via join upstream) as `BanditArmState`
        with one row per (agent_id, arm_name) and columns:
        banditarmstate__agent_id, banditarmstate__arm_name, banditarmstate__pulls,
        banditarmstate__mean_reward, banditarmstate__est_cost
    Output:
      - Adds one ActionProposal per agent (highest UCB score) using DataFrame ops only.
    """

    components = (ObsComponent, BanditArmState, BanditPolicy)
    priority = 5

    def __init__(self, planner_id: str = "bandit", c_ucb: float = 1.5, lambda_cost: float = 0.0):
        self.planner_id = planner_id
        self.c_ucb = c_ucb
        self.lambda_cost = lambda_cost

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Compute total pulls per agent to get t in UCB
        # Group arm rows by agent and sum pulls
        pulls_per_agent = (
            df.groupby(col("banditarmstate__agent_id")).agg({"banditarmstate__pulls": "sum"})
              .rename({
                  "banditarmstate__agent_id": "agent_id",
                  "banditarmstate__pulls_sum": "total_pulls"
              })
        )

        # Join back to arm rows to compute per-arm UCB scores
        arms = df.join(pulls_per_agent, left_on="banditarmstate__agent_id", right_on="agent_id", how="left")

        # UCB expression: mean + c * sqrt(2 ln t / (n+1)) - lambda * est_cost
        from daft import lit
        # coalesce per-agent policy with processor defaults
        c_ucb = col("banditpolicy__c_ucb").fill_null(self.c_ucb)
        lam = col("banditpolicy__lambda_cost").fill_null(self.lambda_cost)
        planner = col("banditpolicy__planner_id").fill_null(self.planner_id)
        arms = (
            arms.with_columns({
                "_n": (col("banditarmstate__pulls") + lit(1)),
                "_t": (col("total_pulls").fill_null(0) + lit(1)),
            })
            .with_column("_explore", (c_ucb * (2.0 * (col("_t").log()) / col("_n")).sqrt()))
            .with_column("_score", col("banditarmstate__mean_reward") + col("_explore") - lam * col("banditarmstate__est_cost"))
        )

        # For each agent_id, pick the arm with max _score
        ranked = arms.sort(col("_score"), desc=True)
        winners = (
            ranked.groupby(col("banditarmstate__agent_id"))
                   .agg({
                       "banditarmstate__arm_name": "first",
                       "obs__tick": "first",
                   })
                   .rename({
                       "banditarmstate__agent_id": "actionproposal__agent_id",
                       "banditarmstate__arm_name_first": "actionproposal__action_name",
                       "obs__tick_first": "actionproposal__tick",
                   })
        )

        # Attach constant columns for planner_id, priority, params_json
        winners = winners.with_columns({
            "actionproposal__planner_id": planner,
            "actionproposal__priority": lit(0.0),
            "actionproposal__params_json": lit("{}"),
        })

        # Join back to original df to produce a dataframe carrying the chosen proposals
        return df.join(winners, left_on="obs__agent_id", right_on="actionproposal__agent_id", how="left")


class ResolveActionsProcessor(AsyncProcessor):
    components = (ActionProposal,)
    priority = 6

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Group by agent and pick max priority; stable by planner_id tie-break

        chosen = (
            df.sort(col("actionproposal__priority"), desc=True)
              .groupby(col("actionproposal__agent_id"))
              .agg({
                  "actionproposal__action_name": "first",
                  "actionproposal__params_json": "first",
                  "actionproposal__planner_id": "first",
                  "actionproposal__tick": "first",
              })
        )
        # Rename and pack into ChosenAction schema
        chosen = chosen.rename({
            "actionproposal__agent_id": "chosenaction__agent_id",
            "actionproposal__action_name": "chosenaction__action_name",
            "actionproposal__params_json": "chosenaction__params_json",
            "actionproposal__planner_id": "chosenaction__planner_id",
            "actionproposal__tick": "chosenaction__tick",
        })
        return df.join(chosen, how="left")


