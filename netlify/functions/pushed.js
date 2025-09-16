import { getStore } from "@netlify/blobs";

export default async () => {
  try {
    const store = getStore("water-push");
    const list = await store.list({ prefix: "push/", limit: 100 });
    const items = [];
    for (const entry of list.blobs || []) {
      const text = await store.get(entry.key, { type: "text" });
      try {
        items.push(JSON.parse(text));
      } catch {
        items.push({ raw: text, key: entry.key });
      }
    }
    return new Response(JSON.stringify({ count: items.length, items }), {
      headers: { "content-type": "application/json" },
    });
  } catch (e) {
    return new Response(
      JSON.stringify({ error: "Blob store unavailable", detail: String(e) }),
      { status: 500 }
    );
  }
};

export const config = {
  path: "/pushed",
}; 