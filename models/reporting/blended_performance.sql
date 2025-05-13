{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH blended_data as
    (SELECT channel, date::date, date_granularity, campaign_name, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        COALESCE(SUM(sho_purchases),0) as sho_purchases, COALESCE(SUM(sho_ft_purchases),0) as sho_ft_purchases, COALESCE(SUM(sho_revenue),0) as sho_revenue, COALESCE(SUM(sho_ft_revenue),0) as sho_ft_revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            campaign_name, spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity,
            campaign_name, spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'TikTok' as channel, date, date_granularity,
            campaign_name, spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue
        FROM {{ source('reporting','tiktok_ad_performance') }}
        UNION ALL
        SELECT 'Shopify' as channel, date, date_granularity,
        null as campaign_name, 0 as spend, 0 as clicks, 0 as impressions, 0 as paid_purchases, 0 as paid_revenue,
            count(*) as sho_purchases, sum(case when customer_order_index = 1 then 1 else 0 end) as sho_ft_purchases, 
            sum(subtotal_revenue) as sho_revenue, sum(case when customer_order_index = 1 then subtotal_revenue else 0 end) as sho_ft_revenue
        FROM (
        SELECT * FROM {{ source('reporting','shopify_orders') }}
	    WHERE order_id NOT IN (
            select order_id from (select order_id, count(*)
	        from {{ source('reporting','shopify_line_items') }}
	        where product_title = 'Probiotic Underarm Toner' and quantity > 5
	        group by 1)
            )
        )
        )
    GROUP BY channel, date, date_granularity, campaign_name)
    
SELECT channel,
    date,
    date_granularity,
    campaign_name,
    spend,
    clicks,
    impressions,
    paid_purchases,
    paid_revenue,
    sho_purchases,
    sho_ft_purchases,
    sho_revenue,
    sho_ft_revenue
FROM blended_data
