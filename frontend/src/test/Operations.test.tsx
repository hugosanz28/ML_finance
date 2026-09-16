import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Operations } from "../Operations";
import { ApiError } from "../api";
import { AuditView } from "../operation-results";
import { csvUploads, opsApi, type Health, type Job } from "../operations-api";

const health: Health = {
  status: "ok",
  api_version: "v1",
  mode: "operations",
  workspace_mode: "demo",
};
const hash = "sha256:" + "a".repeat(64);
const job: Job = {
  job_id: "a".repeat(32),
  operation: "import",
  state: "succeeded",
  progress: 100,
  phase: "finished",
  created_at: "2026-04-30",
  started_at: null,
  finished_at: null,
  warnings: [],
  error_code: null,
  result: { status: "succeeded", message: "Importación completada" },
};
function setup(status: Health = health) {
  vi.spyOn(opsApi, "health").mockResolvedValue(status);
  vi.spyOn(opsApi, "jobs").mockResolvedValue({ jobs: [] });
  vi.spyOn(opsApi, "brief").mockResolvedValue({
    content: "Mi plan",
    content_hash: hash,
    exists: true,
  });
  vi.spyOn(opsApi, "targets").mockResolvedValue({
    portfolio_targets: { base_currency: "EUR" },
    content_hash: hash,
    exists: true,
    target_weights: {},
    validation_error: null,
  });
  const send = vi.spyOn(opsApi, "send").mockResolvedValue(job);
  render(<Operations health={status} visible />);
  return send;
}
describe("operational safety", () => {
  it("requires confirmation, preserves the environment, and cancels without POST", async () => {
    const send = setup();
    const user = userEvent.setup();
    await user.click(
      screen.getByRole("button", { name: "Revisar importación" }),
    );
    expect(send).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "Cancelar" }));
    expect(send).not.toHaveBeenCalled();
    await user.click(
      screen.getByRole("button", { name: "Revisar importación" }),
    );
    await user.click(
      screen.getByRole("button", { name: "Confirmar y ejecutar" }),
    );
    await waitFor(() => expect(send).toHaveBeenCalledTimes(1));
    expect(send.mock.calls[0]?.[1]).toEqual({
      workspace_mode: "demo",
      confirm: true,
    });
  });
  it("does not write when the API is read-only or unverified", () => {
    const send = setup({ ...health, mode: "read_only" });
    expect(
      screen.getByRole("button", { name: "Revisar importación" }),
    ).toBeDisabled();
    expect(send).not.toHaveBeenCalled();
  });
  it("retains exactly the same key and body after an ambiguous submission", async () => {
    const send = setup();
    send.mockRejectedValueOnce(new ApiError("connection_failed"));
    const user = userEvent.setup();
    await user.click(
      screen.getByRole("button", { name: "Revisar importación" }),
    );
    await user.click(
      screen.getByRole("button", { name: "Confirmar y ejecutar" }),
    );
    await screen.findByText(/Envío sin confirmar/);
    expect(
      screen.queryByRole("button", { name: "Cancelar" }),
    ).not.toBeInTheDocument();
    await user.click(
      screen.getByRole("button", {
        name: "Consultar envío con la misma clave",
      }),
    );
    await waitFor(() => expect(send).toHaveBeenCalledTimes(2));
    expect(send.mock.calls[1]?.slice(0, 3)).toEqual(
      send.mock.calls[0]?.slice(0, 3),
    );
  });
  it("rechecks server mode before sending", async () => {
    const send = setup();
    vi.mocked(opsApi.health).mockResolvedValue({
      ...health,
      workspace_mode: "real",
    });
    const user = userEvent.setup();
    await user.click(
      screen.getByRole("button", { name: "Revisar importación" }),
    );
    await user.click(
      screen.getByRole("button", { name: "Confirmar y ejecutar" }),
    );
    await screen.findByText(/El entorno del servidor cambió/);
    expect(send).not.toHaveBeenCalled();
  });
  it("has no external provider choices in demo and requires explicit selection in real", async () => {
    setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Agentes" }));
    expect(
      screen.queryByRole("option", { name: /OpenAI/ }),
    ).not.toBeInTheDocument();
    expect(screen.getByLabelText("Proveedor del modelo")).toHaveValue("static");
    expect(screen.getByLabelText("Proveedor de búsqueda")).toHaveValue("null");
  });
  it("invalidates hashes after a conflict and requires explicit reload", async () => {
    const send = setup();
    const failed = {
      ...job,
      operation: "brief",
      state: "failed" as const,
      error_code: "content_conflict",
    };
    send.mockResolvedValue(failed);
    vi.mocked(opsApi.jobs).mockResolvedValue({ jobs: [failed] });
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Configuración" }));
    await screen.findByDisplayValue("Mi plan");
    await user.click(screen.getByRole("button", { name: "Revisar plan" }));
    await user.click(
      screen.getByRole("button", { name: "Confirmar y ejecutar" }),
    );
    await screen.findByText(/El contenido cambió/);
    expect(screen.getByRole("button", { name: "Revisar plan" })).toBeDisabled();
    expect(send.mock.calls[0]?.[1].expected_previous_hash).toBe(hash);
    await user.click(
      screen.getByRole("button", { name: /Recargar configuración/ }),
    );
    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: "Revisar plan" }),
      ).toBeEnabled(),
    );
  });
  it("only exposes retry for failed simulations", async () => {
    setup();
    vi.mocked(opsApi.jobs).mockResolvedValue({
      jobs: [
        { ...job, state: "failed" },
        {
          ...job,
          job_id: "b".repeat(32),
          operation: "simulation",
          state: "failed",
        },
      ],
    });
    const user = userEvent.setup();
    await user.click(
      screen.getByRole("button", { name: "Actualizar ejecuciones" }),
    );
    await user.click(screen.getByRole("button", { name: "Ejecuciones" }));
    expect(
      await screen.findAllByRole("button", { name: "Reintentar simulación" }),
    ).toHaveLength(1);
  });
  it("rejects oversized uploads before reading bytes", async () => {
    const file = new File(["x"], "data.csv");
    Object.defineProperty(file, "size", { value: 6 * 1024 * 1024 });
    await expect(csvUploads([file])).rejects.toThrow("invalid_uploads");
  });
  it("serializes configuration reads to avoid competing for the server lock", async () => {
    setup();
    let release!: (value: Awaited<ReturnType<typeof opsApi.brief>>) => void;
    vi.mocked(opsApi.brief).mockReturnValue(
      new Promise((resolve) => {
        release = resolve;
      }),
    );
    await userEvent.click(
      screen.getByRole("button", { name: "Configuración" }),
    );
    expect(opsApi.targets).not.toHaveBeenCalled();
    await act(async () =>
      release({ content: "Plan secuencial", content_hash: hash, exists: true }),
    );
    await screen.findByDisplayValue("Plan secuencial");
    expect(opsApi.targets).toHaveBeenCalledOnce();
  });
  it("requires explicit real sources and retains optional dates in the confirmed payload", async () => {
    const send = setup({ ...health, workspace_mode: "real" });
    const user = userEvent.setup();
    expect(screen.getByLabelText("Fuente de precios y FX")).toHaveValue("");
    await user.selectOptions(
      screen.getByLabelText("Fuente de precios y FX"),
      "yfinance",
    );
    await user.click(
      screen.getByRole("button", { name: "Revisar actualización" }),
    );
    await user.click(
      screen.getByRole("button", { name: "Confirmar y ejecutar" }),
    );
    await waitFor(() => expect(send).toHaveBeenCalledOnce());
    expect(send.mock.calls[0]?.[1]).toEqual({
      workspace_mode: "real",
      confirm: true,
      fx_provider: "yfinance",
      price_provider: "yfinance",
      start_date: null,
      end_date: null,
      scope: "both",
      only_missing_base: false,
    });
  });
  it("renders legacy audit and untrusted HTML as inert text", async () => {
    render(
      <AuditView
        audit={{
          run_id: "legacy",
          schema_version: 1,
          is_legacy: true,
          compatibility_warnings: ["missing_provider_metadata"],
          preflight: {},
          input_payload: {},
          run_metadata: {},
          agents: {
            analista: {
              prompt_rendered: '<img src="https://example.com/track">',
            },
          },
        }}
      />,
    );
    await userEvent.click(screen.getByText("analista"));
    await userEvent.click(screen.getByText("Prompt renderizado"));
    expect(
      within(
        screen.getByRole("region", { name: "Auditoría de agentes" }),
      ).queryByRole("img"),
    ).not.toBeInTheDocument();
    expect(screen.getByText(/https:\/\/example.com\/track/)).toBeVisible();
  });
});
