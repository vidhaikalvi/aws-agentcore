import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from bedrock_agentcore_starter_toolkit.operations.memory.manager import MemoryManager
from bedrock_agentcore_starter_toolkit.services.runtime import BedrockAgentCoreClient
from bedrock_agentcore_starter_toolkit.utils.runtime.config import load_config, save_config
from bedrock_agentcore_starter_toolkit.utils.runtime.schema import BedrockAgentCoreAgentSchema, BedrockAgentCoreConfigSchema
from bedrock_agentcore_starter_toolkit.operations.runtime.exceptions import RuntimeToolkitException
from bedrock_agentcore_starter_toolkit.operations.runtime.models import DestroyResult

from bedrock_agentcore_starter_toolkit.operations.runtime.destroy import (_destroy_agentcore_endpoint, _destroy_agentcore_agent, _destroy_codebuild_project, _destroy_ecr_images, _cleanup_agent_config)

log = logging.getLogger(__name__)


def destroy_bedrock_agentcore(
    config_path: Path,
    agent_name: Optional[str] = None,
    dry_run: bool = False,
    force: bool = False,
    delete_ecr_repo: bool = False,
) -> DestroyResult:
    """Destroy Bedrock AgentCore resources.

    Args:
        config_path: Path to the configuration file
        agent_name: Name of the agent to destroy (default: use default agent)
        dry_run: If True, only show what would be destroyed without actually doing it
        force: If True, skip confirmation prompts
        delete_ecr_repo: If True, also delete the ECR repository after removing images

    Returns:
        DestroyResult: Details of what was destroyed or would be destroyed

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        ValueError: If agent is not found or not deployed
        RuntimeError: If destruction fails
    """
    log.info(
        "Starting destroy operation for agent: %s (dry_run=%s, delete_ecr_repo=%s)",
        agent_name or "default",
        dry_run,
        delete_ecr_repo,
    )

    try:
        # Load configuration
        project_config = load_config(config_path)
        agent_config = project_config.get_agent_config(agent_name)

        if not agent_config:
            raise ValueError(f"Agent '{agent_name or 'default'}' not found in configuration")

        # Initialize result
        result = DestroyResult(agent_name=agent_config.name, dry_run=dry_run)

        # Check if agent is deployed
        if not agent_config.bedrock_agentcore:
            result.warnings.append("Agent is not deployed, nothing to destroy")
            return result

        # Initialize AWS session and clients
        session = boto3.Session(region_name=agent_config.aws.region)

        # 1. Destroy Bedrock AgentCore endpoint (if exists)
        _destroy_agentcore_endpoint(session, agent_config, result, dry_run)

        # 2. Destroy Bedrock AgentCore agent
        _destroy_agentcore_agent(session, agent_config, result, dry_run)

        # 3. Remove ECR images and optionally the repository (only for container deployments)
       
        _destroy_ecr_images(session, agent_config, result, dry_run, delete_ecr_repo)

        # 4. Remove CodeBuild project (only for container deployments)
        _destroy_codebuild_project(session, agent_config, result, dry_run)
    

        # 5. Remove memory resource
        if agent_config.memory and agent_config.memory.memory_id and agent_config.memory.mode != "NO_MEMORY":
            if agent_config.memory.was_created_by_toolkit:
                # Memory was created by toolkit during configure/launch - delete it
                _destroy_memory(session, agent_config, result, dry_run)
                if not dry_run:
                    log.info("Deleted memory (was created by toolkit): %s", agent_config.memory.memory_id)
            else:
                # Memory was pre-existing - preserve it
                result.warnings.append(f"Memory {agent_config.memory.memory_id} preserved (was pre-existing)")
                log.info("Preserving pre-existing memory: %s", agent_config.memory.memory_id)

        # 6. Remove CodeBuild IAM Role (only for container deployments)
        # if agent_config.deployment_type == "container":
        #     _destroy_codebuild_iam_role(session, agent_config, result, dry_run)
        # else:
        #     log.info("Skipping CodeBuild IAM role cleanup for direct_code_deploy deployment")

        # 7. Remove IAM execution role (if not used by other agents)
        # _destroy_iam_role(session, project_config, agent_config, result, dry_run)

        # 8. Clean up configuration
        # if not dry_run and not result.errors:
        #     _cleanup_agent_config(config_path, project_config, agent_config.name, result)

        log.info(
            "Destroy operation completed. Resources removed: %d, Warnings: %d, Errors: %d",
            len(result.resources_removed),
            len(result.warnings),
            len(result.errors),
        )

        return result

    except Exception as e:
        log.error("Destroy operation failed: %s", str(e))
        raise RuntimeToolkitException(f"Destroy operation failed: {e}") from e