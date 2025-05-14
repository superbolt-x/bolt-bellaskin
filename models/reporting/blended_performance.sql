{{ config (
    alias = target.database + '_blended_performance'
)}}

{{ config(materialized='table') }}

with shopify_orders_filtered as (

    select * 
    from {{ source('reporting', 'shopify_orders') }}
    where order_id not in (
        select order_id
        from (
            select order_id, sum(quantity) as toner_quantity
            from {{ source('reporting', 'shopify_line_items') }}
            where product_title IN ('Probiotic Underarm Toner','Rose Neroli Hydrating Toner')
            group by order_id
            having toner_quantity > 5
        )
    )

)
    
, blended_data as
    (SELECT channel, date::date, date_granularity, campaign_name, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue, 
        COALESCE(SUM(sho_purchases),0) as sho_purchases, COALESCE(SUM(sho_ft_purchases),0) as sho_ft_purchases, COALESCE(SUM(sho_revenue),0) as sho_revenue, 
        COALESCE(SUM(sho_ft_revenue),0) as sho_ft_revenue, COALESCE(SUM(sho_revenue_excl_toners),0) as sho_revenue_excl_toners
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            campaign_name, spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue, 0 as sho_revenue_excl_toners
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity,
            campaign_name, spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue, 0 as sho_revenue_excl_toners
        FROM {{ source('reporting','googleads_campaign_performance') }}
        UNION ALL
        SELECT 'TikTok' as channel, date, date_granularity,
            campaign_name, spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue,
            0 as sho_purchases, 0 as sho_ft_purchases, 0 as sho_revenue, 0 as sho_ft_revenue, 0 as sho_revenue_excl_toners
        FROM {{ source('reporting','tiktok_ad_performance') }}
        UNION ALL
        SELECT 'Shopify' as channel, date, date_granularity,
            null as campaign_name, 0 as spend, 0 as clicks, 0 as impressions, 0 as paid_purchases, 0 as paid_revenue,
            orders as sho_purchases, first_orders as sho_ft_purchases, subtotal_sales as sho_revenue, first_order_subtotal_sales as sho_ft_revenue, 0 as sho_revenue_excl_toners
        FROM {{ source('reporting','shopify_sales') }}
        UNION ALL
        select 
        'Shopify' as channel,
        date_trunc('day', date) as date,
        'day' as date_granularity,
        null as campaign_name,
        sum(0) as spend,
        sum(0) as clicks,
        sum(0) as impressions,
        sum(0) as paid_purchases,
        sum(0) as paid_revenue,
        sum(0) as sho_purchases,
        sum(0) as sho_ft_purchases,
        sum(0) as sho_revenue,
        sum(0) as sho_ft_revenue,
        sum(subtotal_revenue) as sho_revenue_excl_toners
        from shopify_orders_filtered
        group by 1, 2, 3
        UNION ALL
        select 
        'Shopify' as channel,
        date_trunc('week', date) as date,
        'week' as date_granularity,
        null as campaign_name,
        sum(0) as spend,
        sum(0) as clicks,
        sum(0) as impressions,
        sum(0) as paid_purchases,
        sum(0) as paid_revenue,
        sum(0) as sho_purchases,
        sum(0) as sho_ft_purchases,
        sum(0) as sho_revenue,
        sum(0) as sho_ft_revenue,
        sum(subtotal_revenue) as sho_revenue_excl_toners
        from shopify_orders_filtered
        group by 1, 2, 3
        UNION ALL
        select 
        'Shopify' as channel,
        date_trunc('month', date) as date,
        'month' as date_granularity,
        null as campaign_name,
        sum(0) as spend,
        sum(0) as clicks,
        sum(0) as impressions,
        sum(0) as paid_purchases,
        sum(0) as paid_revenue,
        sum(0) as sho_purchases,
        sum(0) as sho_ft_purchases,
        sum(0) as sho_revenue,
        sum(0) as sho_ft_revenue,
        sum(subtotal_revenue) as sho_revenue_excl_toners
        from shopify_orders_filtered
        group by 1, 2, 3
        UNION ALL
        select 
        'Shopify' as channel,
        date_trunc('quarter', date) as date,
        'quarter' as date_granularity,
        null as campaign_name,
        sum(0) as spend,
        sum(0) as clicks,
        sum(0) as impressions,
        sum(0) as paid_purchases,
        sum(0) as paid_revenue,
        sum(0) as sho_purchases,
        sum(0) as sho_ft_purchases,
        sum(0) as sho_revenue,
        sum(0) as sho_ft_revenue,
        sum(subtotal_revenue) as sho_revenue_excl_toners
        from shopify_orders_filtered
        group by 1, 2, 3
        UNION ALL
        select 
        'Shopify' as channel,
        date_trunc('year', date) as date,
        'year' as date_granularity,
        null as campaign_name,
        sum(0) as spend,
        sum(0) as clicks,
        sum(0) as impressions,
        sum(0) as paid_purchases,
        sum(0) as paid_revenue,
        sum(0) as sho_purchases,
        sum(0) as sho_ft_purchases,
        sum(0) as sho_revenue,
        sum(0) as sho_ft_revenue,
        sum(subtotal_revenue) as sho_revenue_excl_toners
        from shopify_orders_filtered
        group by 1, 2, 3)
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
    sho_ft_revenue,
    sho_revenue_excl_toners
FROM blended_data
