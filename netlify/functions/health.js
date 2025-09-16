export default async () => {
  return new Response(
    JSON.stringify({ status: "ok", time: new Date().toISOString() }),
    { headers: { "content-type": "application/json" } }
  );
};

export const config = {
  path: "/health",
}; 