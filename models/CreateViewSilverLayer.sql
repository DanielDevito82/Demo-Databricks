-- models/v_post_comments.sql

SELECT
    p.*,
    c.*
FROM
    {{ ref('t_reddit_posts') }} p
LEFT JOIN
    {{ ref('t_reddit_comments') }} c
ON
    p.id = c.id;