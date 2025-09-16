export default async (req) => {
  const proto = req.headers.get("x-forwarded-proto") || "https";
  const host = req.headers.get("x-forwarded-host") || req.headers.get("host") || "";
  const base = `${proto}://${host}`;
  const html = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>用户用水行为系统 - API 网关</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif; padding: 24px; line-height: 1.6; }
    code, pre { background: #f5f7fa; border: 1px solid #e5e9f2; border-radius: 6px; padding: 12px; display: block; overflow-x: auto; }
    a { color: #2563eb; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .card { border: 1px solid #e5e9f2; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
    .muted { color: #6b7280; font-size: 13px; }
  </style>
</head>
<body>
  <h1>用户用水行为系统 · 公网推送入口</h1>
  <p class="muted">将以下地址提供给厂家用于数据推送；支持 JSON 与表单两种方式。</p>

  <div class="card">
    <h2>推送地址</h2>
    <ul>
      <li><strong>JSON 推送</strong>：<a href="${base}/api/data">${base}/api/data</a>（POST JSON）</li>
      <li><strong>表单推送</strong>：<a href="${base}/api/data_compat">${base}/api/data_compat</a>（POST x-www-form-urlencoded / multipart）</li>
      <li><strong>健康检查</strong>：<a href="${base}/health">${base}/health</a></li>
      <li><strong>最近推送列表</strong>：<a href="${base}/pushed">${base}/pushed</a></li>
      <li><strong>公共信息</strong>：<a href="${base}/public_info">${base}/public_info</a></li>
    </ul>
  </div>

  <div class="card">
    <h2>curl 示例</h2>
    <p><strong>JSON：</strong></p>
    <pre><code>curl -X POST '${base}/api/data' \
  -H 'Content-Type: application/json' \
  -d '{"deviceNo":"123456","flow":12.3,"updateTime":"2025-09-08 12:00:00"}'
</code></pre>

    <p><strong>表单：</strong></p>
    <pre><code>curl -X POST '${base}/api/data_compat' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'deviceNo=123456&flow=12.3&updateTime=2025-09-08+12:00:00'
</code></pre>
  </div>

  <p class="muted">本页面由 Netlify Functions 提供；数据将存储在 Netlify Blobs（命名空间：water-push）。</p>
</body>
</html>`;

  return new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } });
};

export const config = {
  path: "/",
}; 