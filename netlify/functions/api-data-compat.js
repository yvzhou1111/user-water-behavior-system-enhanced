import { getStore } from "@netlify/blobs";

export default async (req) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405 });
  }

  const contentType = req.headers.get("content-type") || "";
  let payload = {};

  if (contentType.includes("application/x-www-form-urlencoded")) {
    const text = await req.text();
    const params = new URLSearchParams(text);
    params.forEach((v, k) => (payload[k] = v));
  } else if (contentType.includes("multipart/form-data")) {
    const form = await req.formData?.();
    if (form) {
      for (const [k, v] of form.entries()) {
        payload[k] = typeof v === "string" ? v : v?.name || "blob";
      }
    }
  } else {
    try {
      payload = await req.json();
    } catch {
      const text = await req.text();
      payload = { raw: text };
    }
  }

  const deviceNo = payload.deviceNo || payload.device_id || payload.device || "unknown";
  const now = new Date().toISOString();
  const record = { now, ip: req.headers.get("x-forwarded-for") || "", payload };

  try {
    const store = getStore("water-push");
    const key = `push/${deviceNo}/${Date.now()}.json`;
    await store.set(key, JSON.stringify(record), { contentType: "application/json" });
  } catch (e) {
    return new Response(
      JSON.stringify({ error: "Blob store unavailable", detail: String(e) }),
      { status: 500 }
    );
  }

  return new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } });
};

export const config = {
  path: "/api/data_compat",
}; 