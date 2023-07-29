# Archived 
Due to changes to Twitter in 2023, this code no longer works. If things change (e.g., [snscrape](https://github.com/JustAnotherArchivist/snscrape) is updated, another tool emerges, etc.), I might fix this. Huge thanks to [JustAnotherArchivist](https://github.com/JustAnotherArchivist) for their amazing work with snscrape.



# Twitter Archiver for Third-party Accounts
This program uses [snscrape](https://github.com/JustAnotherArchivist/snscrape) to archive all publicly available tweets from a list of Twitter users (or, with some minor modifications, results from (advanced) Twitter searches) to a local [SQLite](https://www.sqlite.org/index.html) DB, along with: 
* Entire chains of retweets, quoted tweets replied to tweets 
* Media (videos, photos, etc.)

[snscrape](https://github.com/JustAnotherArchivist/snscrape) easily bypasses Twitter's 3200 tweet limit, with no dev account or API keys required. I've tried several tools, and snscrape's the first one to fully work - a big thanks to its maintainers!

**Limitations**
* Tweets or user accounts that have been/are deleted, suspended or private cannot be saved
* Twitter may rate limit or block your IP, though I have not experienced this

# Installation Steps
1. Run either in Docker or via the CLI
2. Add the account(s) to be archived in the following folderss:
     - Docker: Update `docker-compose.yml` -> TWITTER_USERS
     - CLI: Update `modules/settings.py` -> TWITTER_ACCOUNTS

**Optional Steps**
1. If you do **not** want to save retweets, remove `include:nativeretweets`
2. Utilize other advanced search queries (documentation on how to do this is in the code, beneath the `TWITTER_ACCOUNTS` var declaration)
3. You may want to run this script via VPN or proxy 
4. When finished, compress the database

# Areas for Improvement
I don't have any major plans to improve this; however, create an issue or PR if you think a function should be added, code refractored, etc. 

# Why I've Made This
One important aspect of Twitter is it's been a fabulous way of tracking events - when people knew, said or did things. This is important for the historical record, preventing authoritarian gaslighting, holding people accountable, and learning/improving. 

There are also accounts that are just interesting, bring me joy and laughter, etc; and I'd be saddened at being unable to see those again.  


🇺🇦 Слава Україні - Glory to Ukraine
