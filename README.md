# Twitter Archiver for Third-party Accounts
This script utilizes [snscrape](https://github.com/JustAnotherArchivist/snscrape) to archive all tweets from an individual Twitter user's accounts to a local [SQLite](https://www.sqlite.org/index.html) DB. [snscrape](https://github.com/JustAnotherArchivist/snscrape) easily bypasses Twitter's 3200 tweet limit, with no dev account or API keys required. I've tried several tools, and snscrape's the first one to fully work - a big thanks to its maintainers!

**Limitations**
* Tweets or user accounts that have been/are deleted, suspended or private cannot be saved
* Twitter may rate limit or block your IP, though I have not experienced this

# Installation Steps (archiver.py)
1. Install the Python module for snscrape. 
     - As of the time of writing, this can be done via `pip3 install snscrape`
     - Check [snscrape's repo](https://github.com/JustAnotherArchivist/snscrape) for current instructions
2. Download `archiver.py`
3. Add the account(s) you want to archive to the `TWITTER_ACCOUNTS = ["example1", "example2", "example3"]` list

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
