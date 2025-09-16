export default async (req) => {
  const proto = req.headers.get("x-forwarded-proto") || "https";
  const host = req.headers.get("x-forwarded-host") || req.headers.get("host") || "";
  const base = `${proto}://${host}`;
  const data = {
    base,
    push_url_json: `${base}/api/data`,
    push_url_form: `${base}/api/data_compat`,
  };
  return new Response(JSON.stringify(data), {
    headers: { "content-type": "application/json" },
  });
};

export const config = {
  path: "/public_info",
}; 