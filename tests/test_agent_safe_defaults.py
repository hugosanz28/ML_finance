from src.agents.analista_activos import AnalistaActivosAgent
from src.agents.asistente_aportacion_mensual import AsistenteAportacionMensualAgent
from src.agents.monitor_tematico import MonitorTematicoAgent
from scripts.run_monitor_tematico import parse_args


def test_direct_agent_construction_uses_offline_providers() -> None:
    monitor = MonitorTematicoAgent()
    analyst = AnalistaActivosAgent()
    contribution = AsistenteAportacionMensualAgent()

    assert monitor.search_provider.name == "null"
    assert monitor.llm_provider.name == "static_llm"
    assert analyst.llm_provider.name == "static_llm"
    assert contribution.llm_provider.name == "static_llm"


def test_monitor_cli_defaults_to_offline_providers(monkeypatch) -> None:
    monkeypatch.setattr("sys.argv", ["run_monitor_tematico.py"])

    args = parse_args()

    assert args.llm_provider == "static"
    assert args.search_provider == "null"
