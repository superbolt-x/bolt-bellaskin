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
            select order_id,count(*)
            from {{ source('reporting', 'shopify_line_items') }}
            where product_title = 'Probiotic Underarm Toner'
              and quantity > 5
            group by order_id
        )
    )

)

, unioned as (

    {% set granularities = ['day', 'week', 'month', 'quarter', 'year'] %}

    {% for granularity in granularities %}
    
    select 
        'Shopify' as channel,
        date_trunc('{{ granularity }}', date) as date,
        '{{ granularity }}' as date_granularity,
        null as campaign_name,
        0 as spend,
        0 as clicks,
        0 as impressions,
        0 as paid_purchases,
        0 as paid_revenue,
        count(*) as sho_purchases,
        sum(case when customer_order_index = 1 then 1 else 0 end) as sho_ft_purchases,
        sum(subtotal_revenue) as sho_revenue,
        sum(case when customer_order_index = 1 then subtotal_revenue else 0 end) as sho_ft_revenue
    from shopify_orders_filtered
    group by 1, 2, 3

    {% if not loop.last %}
    union all
    {% endif %}

    {% endfor %}

)
    
, blended_data as
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
        SELECT * FROM unioned
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
