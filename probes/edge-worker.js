export default {
    async scheduled(event, env, ctx) {
      ctx.waitUntil(sendTelemetry(env));
    },
    
    // Also triggerable via HTTP for testing
    async fetch(request, env, ctx) {
      await sendTelemetry(env);
      return new Response("Telemetry sent", { status: 200 });
    }
  };
  
  async function sendTelemetry(env) {
    const region = request.cf?.colo || "UNKNOWN";
    const start = Date.now();
    
    try {
      // Simulate checking an edge dependent service or just record script latency
      await new Promise(resolve => setTimeout(resolve, Math.random() * 50));
      
      const latency = Date.now() - start;
      const payload = {
        region: `CF_${region}`,
        latency: latency,
        timestamp: new Date().toISOString()
      };
  
      // Send to our Gateway
      const destUrl = env.GATEWAY_URL || "https://chaos.jeshwanth.dev/telemetry";
      
      const response = await fetch(destUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
      });
  
      if (!response.ok) {
        console.error(`Telemetry failed with status: ${response.status}`);
      }
    } catch (err) {
      console.error(`Telemetry error: ${err.message}`);
    }
  }
