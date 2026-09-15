// @vitest-environment node
import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  analyticsSchema,
  definitionsSchema,
  portfolioSchema,
} from "../contracts";

// CI regenerates this payload from FastAPI after bootstrapping the offline demo.
const path = resolve("../.test_tmp/frontend-api.json");
describe.skipIf(!existsSync(path))(
  "live Python → FastAPI → TypeScript contract",
  () => {
    it("validates every benchmark and period without financial client calculations", () => {
      const payload = z
        .object({
          analytics: z.array(analyticsSchema),
          portfolio: portfolioSchema,
          definitions: definitionsSchema,
        })
        .parse(JSON.parse(readFileSync(path, "utf-8")));
      expect(payload.analytics).toHaveLength(16);
      expect(
        payload.analytics.every(
          (item) => item.period.end_date === payload.portfolio.as_of_date,
        ),
      ).toBe(true);
      expect(payload.portfolio.positions.length).toBeGreaterThan(0);
    });
  },
);
