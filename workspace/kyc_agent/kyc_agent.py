import os

import boto3

from bedrock_agentcore.identity.auth import requires_access_token
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from bedrock_agentcore.runtime.context import RequestContext

from mcp.client.streamable_http import streamablehttp_client

from strands import Agent, tool
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient


KYC_RESEARCH_AGENT_PROMPT = """You are a Know Your Customer (KYC) research agent whose job is to gather information about mortgage applicants to help determine their eligibility for a primary residence mortgage.
You have access to the following tools:
1. Credit Report Search: Use this tool to search credit report data by fields 'full_legal_name' or 'primary_address'.
2. Income Verification Search: Use this tool to search income verification data by fields 'employee_name'.
3. Property Records Search: Use this tool to search property records data by fields 'owner_name_on_deed' or 'property_address'.
4. Lien Records Search: Use this tool to search lien records data by fields 'debtor_name' or 'debtor_address'.
5. Calculator: Use this tool to perform mathematical calculations, including distance calculations.

Gather the following information about the applicant:
1. Full legal name
2. Current address
3. Employment status and income
4. Credit score
5. Debt to income ratio
6. Any liens or judgments against them

Note that the applicant's name and address may be represented in different ways in different datasets. Use fuzzy matching and partial matches across datasets to ensure you find all relevant information.

Use the tools at your disposal to gather the necessary information to help support the mortgage verification decision.
"""

KYC_AGENT = None
MCP_CLIENT = None
MODEL = BedrockModel(
    model_id=os.getenv("MODEL_ID", "global.anthropic.claude-haiku-4-5-20251001-v1:0"),
    temperature=0.0,
)

APP = BedrockAgentCoreApp()

# this decorator will obtain an OAuth2 access token using the M2M flow and inject it
# into the decorated function as the 'oauth2_token' parameter
@requires_access_token(
    provider_name=os.getenv("OAUTH2_ID_PROVIDER", ""),
    scopes=["gateway/invoke"],
    auth_flow="M2M",
    into="oauth2_token",
)
def initialize_kyc_agent_and_tools(
    model: BedrockModel, system_prompt: str, mcp_url: str, oauth2_token: str
) -> tuple[MCPClient, Agent]:

    mcp_client = MCPClient(
        lambda: streamablehttp_client(
            mcp_url, headers={"Authorization": f"Bearer {oauth2_token}"}
        )
    )

    with mcp_client:
        tools = mcp_client.list_tools_sync()

        agent = Agent(model=model, system_prompt=system_prompt, tools=tools)

    return mcp_client, agent


@APP.entrypoint
async def invoke_kyc_agent(payload: dict[str, str]):

    global KYC_AGENT, MCP_CLIENT

    mcp_url = os.getenv("MCP_URL", None)

    if mcp_url is None:
        raise ValueError("MCP_URL environment variable is not set")

    if KYC_AGENT is None or MCP_CLIENT is None:
        MCP_CLIENT, KYC_AGENT = initialize_kyc_agent_and_tools(
            model=MODEL,
            system_prompt=KYC_RESEARCH_AGENT_PROMPT,
            mcp_url=mcp_url,
        )

    with MCP_CLIENT:

        # instead of response = str(KYC_AGENT(payload["input"]))
        # we will stream the response
        async for event in KYC_AGENT.stream_async(payload["input"]):
            if "data" in event:
                yield event["data"]


if __name__ == "__main__":
    APP.run()
