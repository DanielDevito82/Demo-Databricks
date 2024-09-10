CREATE OR REPLACE VIEW portfolio_analyse.bronze_reddit_deltatable.UNION AS
(
select title
from portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataarchitect_posts
)
/*union

select title
from portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataanalyst_posts

*/

