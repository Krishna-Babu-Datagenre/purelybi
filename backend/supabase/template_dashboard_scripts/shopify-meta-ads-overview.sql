-- Shopify + Meta Ads Overview (slug: shopify-metaads, template id a2000000-0000-0000-0000-000000000002)
-- Self-contained: upserts dashboard_templates row, replaces all widget_templates for this template.
-- No Gross Profit (Paid) KPI on this dashboard; Paid Revenue vs Ad Spend uses a single y-axis (Amount ₹).
-- Run against a DB that already has schema from supabase/migrations/001_*.

insert into public.dashboard_templates (id, slug, name, description, platforms, tags, is_active)
values (
    'a2000000-0000-0000-0000-000000000002',
    'shopify-metaads',
    'Shopify + Meta Ads Overview',
    'Compare Meta ad spend to Shopify paid revenue, gross profit, and net profit after ads. See ROAS, funnel performance, cancellations, and whether ad spend is delivering value.',
    '{shopify,meta_ads}',
    '{shopify,meta-ads,e-commerce,template}',
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
where template_id = 'a2000000-0000-0000-0000-000000000002';

insert into public.widget_templates (template_id, title, type, layout, chart_config, data_config, sort_order)
values
(
    'a2000000-0000-0000-0000-000000000002',
    'Paid Revenue (Shopify)',
    'kpi',
    '{"x":0,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"revenue","changeLabel":"financial status: paid"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END)"}'::jsonb,
    1
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Paid Orders',
    'kpi',
    '{"x":4,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"icon":"orders","changeLabel":"financial status: paid"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN 1 ELSE 0 END)"}'::jsonb,
    2
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Average Order Value (Paid)',
    'kpi',
    '{"x":8,"y":0,"w":4,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"aov","changeLabel":"paid orders only"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) * 1.0 / NULLIF(SUM(CASE WHEN financial_status = ''paid'' THEN 1 ELSE 0 END), 0)"}'::jsonb,
    3
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Total Ad Spend',
    'kpi',
    '{"x":0,"y":2,"w":3,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"generic","changeLabel":"Meta (daily rollup)"}'::jsonb,
    '{"source":"meta_daily_insights","aggregation":"sum","field":"spend"}'::jsonb,
    4
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Meta ROAS (Platform)',
    'kpi',
    '{"x":3,"y":2,"w":3,"h":2}'::jsonb,
    '{"value":0,"suffix":"x","icon":"revenue","changeLabel":"Meta-reported revenue / spend"}'::jsonb,
    '{"source":"meta_daily_insights","aggregation":"custom","formula":"SUM(meta_revenue)*1.0/NULLIF(SUM(spend),0)"}'::jsonb,
    5
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Shopify ROAS (Paid Rev / Spend)',
    'kpi',
    '{"x":6,"y":2,"w":3,"h":2}'::jsonb,
    '{"value":0,"suffix":"x","icon":"revenue","changeLabel":"Shopify paid revenue / ad spend"}'::jsonb,
    '{"components":[{"op":"set","source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END)"},{"op":"divide","source":"meta_daily_insights","aggregation":"sum","field":"spend"}]}'::jsonb,
    6
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Net Profit After Ads',
    'kpi',
    '{"x":9,"y":2,"w":3,"h":2}'::jsonb,
    '{"value":0,"prefix":"₹","icon":"revenue","changeLabel":"gross profit (paid) − ad spend"}'::jsonb,
    '{"components":[{"op":"set","source":"shopify_orders","aggregation":"custom","formula":"SUM(CASE WHEN financial_status = ''paid'' THEN order_margin_base ELSE 0 END)"},{"op":"subtract","source":"meta_daily_insights","aggregation":"sum","field":"spend"}]}'::jsonb,
    7
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Paid Revenue vs Ad Spend',
    'line',
    '{"x":0,"y":4,"w":8,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis"},"legend":{"data":["Paid revenue (Shopify)","Meta ad spend"]},"grid":{"left":"3%","right":"4%","bottom":"3%","containLabel":true},"xAxis":{"type":"category","boundaryGap":false,"data":[]},"yAxis":{"type":"value","name":"Amount (₹)"},"series":[{"name":"Paid revenue (Shopify)","type":"line","smooth":true,"areaStyle":{"opacity":0.15},"data":[]},{"name":"Meta ad spend","type":"line","smooth":true,"areaStyle":{"opacity":0.15},"data":[]}]}'::jsonb,
    '{"sources":["shopify_orders","meta_daily_insights"],"query":"WITH days AS ( SELECT DISTINCT date AS d FROM meta_daily_insights UNION SELECT DISTINCT DATE(created_at) AS d FROM shopify_orders ) SELECT days.d AS dt, COALESCE(s.revenue,0) AS shopify_revenue, COALESCE(d.spend,0) AS meta_spend FROM days LEFT JOIN meta_daily_insights d ON d.date = days.d LEFT JOIN ( SELECT DATE(created_at) AS date, SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) AS revenue FROM shopify_orders GROUP BY DATE(created_at) ) s ON s.date = days.d ORDER BY dt","mappings":{"xAxis":"dt","series":[{"field":"shopify_revenue","name":"Paid revenue (Shopify)"},{"field":"meta_spend","name":"Meta ad spend"}]}}'::jsonb,
    8
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Daily ROAS Trend',
    'line',
    '{"x":8,"y":4,"w":4,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis"},"grid":{"left":"10%","right":"5%","bottom":"3%","containLabel":true},"xAxis":{"type":"category","boundaryGap":false,"data":[]},"yAxis":{"type":"value","name":"ROAS"},"series":[{"name":"ROAS","type":"line","smooth":true,"areaStyle":{"opacity":0.2},"lineStyle":{"width":2},"data":[]}]}'::jsonb,
    '{"sources":["shopify_orders","meta_daily_insights"],"query":"WITH days AS ( SELECT DISTINCT date AS d FROM meta_daily_insights UNION SELECT DISTINCT DATE(created_at) AS d FROM shopify_orders ) SELECT days.d AS dt, COALESCE(m.roas, 0) AS roas FROM days LEFT JOIN meta_daily_insights m ON m.date = days.d ORDER BY dt","mappings":{"xAxis":"dt","series":[{"field":"roas","name":"ROAS"}]}}'::jsonb,
    9
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Meta Ads Conversion Funnel',
    'funnel',
    '{"x":0,"y":10,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item","formatter":"{b}: {c}"},"series":[{"type":"funnel","left":"10%","width":"80%","label":{"show":true,"position":"inside"},"sort":"descending","data":[{"value":0,"name":"Impressions"},{"value":0,"name":"Clicks"},{"value":0,"name":"Add to Cart"},{"value":0,"name":"Checkouts"},{"value":0,"name":"Purchases"}]}]}'::jsonb,
    '{"source":"meta_daily_insights","query":"SELECT SUM(impressions) AS impressions, SUM(clicks) AS clicks, SUM(add_to_cart) AS add_to_cart, SUM(checkouts) AS checkouts, SUM(purchases) AS purchases FROM meta_daily_insights","mappings":{"funnel":["impressions","clicks","add_to_cart","checkouts","purchases"]}}'::jsonb,
    10
),
(
    'a2000000-0000-0000-0000-000000000002',
    'ROAS by Campaign',
    'bar',
    '{"x":6,"y":10,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis"},"grid":{"left":"30%","right":"5%","bottom":"3%","containLabel":true},"yAxis":{"type":"category","data":[]},"xAxis":{"type":"value","name":"ROAS"},"series":[{"type":"bar","data":[],"itemStyle":{"borderRadius":[0,4,4,0]}}]}'::jsonb,
    '{"source":"meta_campaign_insights","query":"SELECT campaign_name, roas FROM meta_campaign_insights ORDER BY spend DESC","mappings":{"yAxis":"campaign_name","series":[{"field":"roas"}]}}'::jsonb,
    11
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Top Products by Paid Revenue',
    'bar',
    '{"x":0,"y":16,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"axis"},"grid":{"left":"35%","right":"5%","bottom":"3%","containLabel":true},"yAxis":{"type":"category","data":[],"inverse":true},"xAxis":{"type":"value","name":"Paid revenue by product (₹)"},"series":[{"type":"bar","data":[],"itemStyle":{"borderRadius":[0,4,4,0]}}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT product_names AS product, SUM(CASE WHEN financial_status = ''paid'' THEN total_price ELSE 0 END) AS revenue FROM shopify_orders GROUP BY product_names ORDER BY revenue DESC LIMIT 8","mappings":{"yAxis":"product","series":[{"field":"revenue"}]}}'::jsonb,
    12
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Orders by Payment Method',
    'pie',
    '{"x":6,"y":16,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item","formatter":"{b}: {c} ({d}%)"},"legend":{"orient":"vertical","right":"5%","top":"center"},"series":[{"type":"pie","radius":["40%","70%"],"center":["40%","50%"],"avoidLabelOverlap":true,"label":{"show":false},"emphasis":{"label":{"show":true,"fontSize":14,"fontWeight":"bold"}},"data":[]}]}'::jsonb,
    '{"source":"shopify_orders","query":"SELECT payment_gateways AS gateway, COUNT(*) AS orders FROM shopify_orders GROUP BY payment_gateways ORDER BY orders DESC","mappings":{"series":[{"nameField":"gateway","valueField":"orders"}]}}'::jsonb,
    13
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Ad Spend by Campaign',
    'pie',
    '{"x":0,"y":22,"w":6,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item","formatter":"{b}: ₹{c} ({d}%)"},"legend":{"orient":"vertical","right":"5%","top":"center"},"series":[{"type":"pie","radius":["40%","70%"],"center":["40%","50%"],"avoidLabelOverlap":true,"label":{"show":false},"emphasis":{"label":{"show":true,"fontSize":14,"fontWeight":"bold"}},"data":[]}]}'::jsonb,
    '{"source":"meta_campaign_insights","query":"SELECT campaign_name, spend FROM meta_campaign_insights ORDER BY spend DESC","mappings":{"series":[{"nameField":"campaign_name","valueField":"spend"}]}}'::jsonb,
    14
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Ad Performance: Spend vs Revenue',
    'scatter',
    '{"x":0,"y":28,"w":12,"h":6}'::jsonb,
    '{"tooltip":{"trigger":"item"},"grid":{"left":"5%","right":"5%","bottom":"10%","containLabel":true},"xAxis":{"type":"value","name":"Spend (₹)","nameLocation":"middle","nameGap":30},"yAxis":{"type":"value","name":"Revenue (₹)"},"series":[{"type":"scatter","symbolSize":20,"data":[],"emphasis":{"itemStyle":{"borderColor":"#333","borderWidth":1}}}]}'::jsonb,
    '{"source":"meta_ad_insights","query":"SELECT ad_name, spend, revenue, purchases FROM meta_ad_insights ORDER BY spend DESC LIMIT 15","mappings":{"series":[{"xField":"spend","yField":"revenue","sizeField":"purchases","nameField":"ad_name"}]}}'::jsonb,
    15
),
(
    'a2000000-0000-0000-0000-000000000002',
    'Cancellation Rate',
    'kpi',
    '{"x":0,"y":34,"w":6,"h":2}'::jsonb,
    '{"value":0,"suffix":"%","icon":"generic","changeLabel":"of all Shopify orders"}'::jsonb,
    '{"source":"shopify_orders","aggregation":"custom","formula":"100.0 * SUM(CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)"}'::jsonb,
    16
);
