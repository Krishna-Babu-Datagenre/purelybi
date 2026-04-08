-- Shopify Overview (slug: shopify-orders, template id a1000000-0000-0000-0000-000000000001)
-- Self-contained: upserts dashboard_templates row, replaces all widget_templates for this template.
-- Run against a DB that already has schema from supabase/migrations/001_*.

insert into public.dashboard_templates (id, slug, name, description, platforms, tags, is_active)
values (
    'a1000000-0000-0000-0000-000000000001',
    'shopify-orders',
    'Shopify Overview',
    'Paid revenue (financial status paid, total_price), cancellations, and order trends. Revenue is not net sales — only paid orders count as revenue.',
    '{shopify}',
    '{shopify,orders,e-commerce,template}',
    true
)
on conflict (slug) do update set
    name = excluded.name,
    description = excluded.description,
    platforms = excluded.platforms,
    tags = excluded.tags,
    is_active = excluded.is_active,
    updated_at = now();

delete from public.widget_templates
where template_id = 'a1000000-0000-0000-0000-000000000001';

insert into public.widget_templates (template_id, title, type, layout, chart_config, data_config, sort_order)
values
(
    'a1000000-0000-0000-0000-000000000001',
    'Paid Revenue',
    'kpi',
    '{"x":0,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"revenue","changeLabel":"financial status: paid"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END)"}'::jsonb,
    1
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Paid Orders',
    'kpi',
    '{"x":4,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"icon":"orders","changeLabel":"financial status: paid"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN 1 ELSE 0 END)"}'::jsonb,
    2
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Average Order Value (Paid)',
    'kpi',
    '{"x":8,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"aov","changeLabel":"paid orders only"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN financial_status = ''paid'' THEN 1 ELSE 0 END), 0)"}'::jsonb,
    3
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Unique Customers (Paid)',
    'kpi',
    '{"x":0,"y":2,"w":4,"h":2}'::jsonb,
    '{"value":0,"icon":"customers","changeLabel":"paid orders"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"COUNT(DISTINCT CASE WHEN financial_status = ''paid'' THEN customer_id END)"}'::jsonb,
    4
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Cancelled Orders',
    'kpi',
    '{"x":4,"y":2,"w":4,"h":2}'::jsonb,
    '{"value":0,"icon":"orders","changeLabel":"all-time"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END)"}'::jsonb,
    5
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Cancellation Rate',
    'kpi',
    '{"x":8,"y":2,"w":4,"h":2}'::jsonb,
    '{"value":0,"suffix":"%","icon":"generic","changeLabel":"of all orders"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"100.0 * SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)"}'::jsonb,
    6
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Daily Paid Revenue',
    'line',
    '{"x":0,"y":4,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis","formatter":"{b}<br/>{a}: ₹{c}"},"legend":{"data":["Paid revenue"]},"grid":{"left":"3%","right":"4%","bottom":"3%","containLabel":true},"xAxis":{"type":"category","boundaryGap":false,"name":"Date","nameLocation":"middle","nameGap":28,"data":[]},"yAxis":{"type":"value","name":"Paid revenue (₹)"},"series":[{"name":"Paid revenue","type":"line","smooth":true,"areaStyle":{"opacity":0.3},"lineStyle":{"width":2},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT DATE(created_at) AS dt, SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) AS revenue FROM shopify_orders GROUP BY dt ORDER BY dt","mappings":{"xAxis":"dt","series":[{"field":"revenue","name":"Paid revenue"}]}}'::jsonb,
    7
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Daily Order Count (All)',
    'line',
    '{"x":6,"y":4,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis","formatter":"{b}<br/>{a}: {c} orders"},"legend":{"data":["Orders"]},"grid":{"left":"3%","right":"4%","bottom":"3%","containLabel":true},"xAxis":{"type":"category","boundaryGap":false,"name":"Date","nameLocation":"middle","nameGap":28,"data":[]},"yAxis":{"type":"value","name":"Orders"},"series":[{"name":"Orders","type":"line","smooth":true,"areaStyle":{"opacity":0.2},"lineStyle":{"width":2},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT DATE(created_at) AS dt, COUNT(*) AS order_count FROM shopify_orders GROUP BY dt ORDER BY dt","mappings":{"xAxis":"dt","series":[{"field":"order_count","name":"Orders"}]}}'::jsonb,
    8
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Top Products by Paid Revenue',
    'bar',
    '{"x":0,"y":10,"w":12,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis","formatter":"{b}: ₹{c}"},"grid":{"left":"35%","right":"5%","bottom":"3%","containLabel":true},"yAxis":{"type":"category","data":[],"inverse":true},"xAxis":{"type":"value","name":"Paid revenue by product (₹)"},"series":[{"type":"bar","data":[],"itemStyle":{"borderRadius":[0,4,4,0]}}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT product_names AS product, SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) AS revenue FROM shopify_orders GROUP BY product_names ORDER BY revenue DESC LIMIT 10","mappings":{"yAxis":"product","series":[{"field":"revenue"}]}}'::jsonb,
    9
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Orders by Payment Method',
    'pie',
    '{"x":0,"y":16,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item","formatter":"{b}: {c} orders ({d}%)"},"legend":{"orient":"vertical","right":"5%","top":"center"},"series":[{"type":"pie","radius":"70%","center":["40%","50%"],"avoidLabelOverlap":true,"label":{"show":false},"emphasis":{"label":{"show":true,"fontSize":14,"fontWeight":"bold"}},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT payment_gateways AS gateway, COUNT(*) AS orders FROM shopify_orders GROUP BY payment_gateways ORDER BY orders DESC","mappings":{"series":[{"nameField":"gateway","valueField":"orders"}]}}'::jsonb,
    10
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Orders by Financial Status',
    'pie',
    '{"x":6,"y":16,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item","formatter":"{b}: {c} orders ({d}%)"},"legend":{"orient":"vertical","right":"5%","top":"center"},"series":[{"type":"pie","radius":"70%","center":["40%","50%"],"avoidLabelOverlap":true,"label":{"show":false},"emphasis":{"label":{"show":true,"fontSize":14,"fontWeight":"bold"}},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT financial_status, COUNT(*) AS orders FROM shopify_orders GROUP BY financial_status ORDER BY orders DESC","mappings":{"series":[{"nameField":"financial_status","valueField":"orders"}]}}'::jsonb,
    11
),
(
    'a1000000-0000-0000-0000-000000000001',
    'Daily Cancelled Orders',
    'line',
    '{"x":0,"y":22,"w":12,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis","formatter":"{b}<br/>{a}: {c}"},"legend":{"data":["Cancellations"]},"grid":{"left":"3%","right":"4%","bottom":"3%","containLabel":true},"xAxis":{"type":"category","boundaryGap":false,"name":"Date","nameLocation":"middle","nameGap":28,"data":[]},"yAxis":{"type":"value","name":"Orders"},"series":[{"name":"Cancellations","type":"line","smooth":true,"areaStyle":{"opacity":0.25},"lineStyle":{"width":2},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT DATE(created_at) AS dt, SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END) AS cancellations FROM shopify_orders GROUP BY dt ORDER BY dt","mappings":{"xAxis":"dt","series":[{"field":"cancellations","name":"Cancellations"}]}}'::jsonb,
    12
);
