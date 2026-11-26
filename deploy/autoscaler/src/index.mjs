import { GatewayAutoscalerDO } from "./durable_object.mjs";

export { GatewayAutoscalerDO };

export default {
  async scheduled(_event, env, ctx) {
    const id = env.AUTOSCALER_DO.idFromName(env.FLY_APP_NAME || "air-pg-gateway");
    const stub = env.AUTOSCALER_DO.get(id);
    ctx.waitUntil(
      stub.fetch("https://autoscaler.internal/tick", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nowMs: Date.now() }),
      })
    );
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return Response.json({ status: "ok", mode: env.AUTOSCALER_MODE || "decision_only" });
    }
    const auth = request.headers.get("X-Air-Internal-Token") || "";
    if (env.AIR_INTERNAL_TOKEN && auth !== env.AIR_INTERNAL_TOKEN) {
      return Response.json({ error: "unauthorized" }, { status: 401 });
    }
    const id = env.AUTOSCALER_DO.idFromName(env.FLY_APP_NAME || "air-pg-gateway");
    const stub = env.AUTOSCALER_DO.get(id);
    return stub.fetch(request);
  },
};
