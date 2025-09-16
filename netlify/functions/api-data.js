import { getStore } from "@netlify/blobs";

export default async (req) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405 });
  }

  let payload;
  try {
    payload = await req.json();
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400 });
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
  path: "/api/data",
}; 