"""Agent workflows for recurring portfolio monitoring and decision support."""

from src.agents.base import AgentValidationError, BaseAgent
from src.agents.models import (
    AgentArtifact,
    AgentContext,
    AgentFinding,
    AgentInputRef,
    AgentRequest,
    AgentResult,
    AgentSource,
    build_agent_context,
)
from src.agents.pipeline import (
    MonthlyAgentPipelineResult,
    build_portfolio_metrics_snapshot,
    load_investment_brief,
    run_monthly_agent_pipeline,
)
from src.agents.analista_activos import AnalistaActivosAgent
from src.agents.asistente_aportacion_mensual import AsistenteAportacionMensualAgent
from src.agents.monitor_tematico import MonitorTematicoAgent

__all__ = [
    "AnalistaActivosAgent",
    "AgentArtifact",
    "AgentContext",
    "AgentFinding",
    "AgentInputRef",
    "AgentRequest",
    "AgentResult",
    "AgentSource",
    "AgentValidationError",
    "AsistenteAportacionMensualAgent",
    "BaseAgent",
    "MonitorTematicoAgent",
    "MonthlyAgentPipelineResult",
    "build_portfolio_metrics_snapshot",
    "build_agent_context",
    "load_investment_brief",
    "run_monthly_agent_pipeline",
]
