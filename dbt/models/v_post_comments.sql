-- models/v_post_comments.sql
{{ config(
    materialized='view',
) }}


SELECT id
FROM {{ ref('t_reddit_posts') }} p


-- SELECT
--    p.id,
--    c.id,
--    c.body
--FROM
--    {{ ref('t_reddit_posts') }} p
--LEFT JOIN
--   {{ ref('t_reddit_comments') }} c
--ON
--    p.id = c.id;