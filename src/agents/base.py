"""Execution contract shared by all project agents."""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.agents.models import AgentContext, AgentRequest, AgentResult


class AgentValidationError(ValueError):
    """Raised when an agent request cannot run with the provided context."""


class BaseAgent(ABC):
    """Minimal interface implemented by all portfolio agents."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable agent identifier."""

    @property
    def description(self) -> str:
        """Human-readable description for docs and orchestration."""
        return ""

    def required_inputs(self) -> tuple[str, ...]:
        """Return context input keys required by the agent design."""
        return ()

    def supports(self, request: AgentRequest) -> bool:
        """Return whether this agent can handle one request shape."""
        return True

    def validate_request(self, request: AgentRequest, context: AgentContext) -> None:
        """Validate the incoming request against the shared context."""
        if context.agent_name != self.name:
            raise AgentValidationError(
                f"Agent context name mismatch: expected {self.name}, got {context.agent_name}"
            )

        missing_required_inputs = [key for key in self.required_inputs() if not context.has_input(key)]
        if missing_required_inputs:
            missing_display = ", ".join(sorted(missing_required_inputs))
            raise AgentValidationError(f"Missing required agent inputs in context: {missing_display}")

        missing_requested_inputs = [key for key in request.input_refs if not context.has_input(key)]
        if missing_requested_inputs:
            missing_display = ", ".join(sorted(missing_requested_inputs))
            raise AgentValidationError(f"Unknown request input references: {missing_display}")

        if not self.supports(request):
            raise AgentValidationError(f"Agent {self.name} does not support the provided request")

    def execute(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        """Validate and execute one agent request."""
        self.validate_request(request, context)
        return self.run(request, context)

    @abstractmethod
    def run(self, request: AgentRequest, context: AgentContext) -> AgentResult:
        """Execute one agent request and return a structured result."""
