# Twitter Archiver for Third-party Accounts
This script utilizes [snscrape](https://github.com/JustAnotherArchivist/snscrape) to archive all tweets from an individual Twitter user's accounts to a local [SQLite](https://www.sqlite.org/index.html) DB. [snscrape](https://github.com/JustAnotherArchivist/snscrape) easily bypasses Twitter's 3200 tweet limit, with no dev account or API keys required. 

**Limitations**
* Tweets or user accounts that have been deleted will not be saved
* Twitter may rate limit or block your IP, though I have not yet experienced this
* Media (images, videos, etc.) not locally saved


# Installation Steps
1. Install the Python module for snscrape. 
     - As of the time of writing, this can be done via `pip3 install snscrape`
     - Check [snscrape's repo](https://github.com/JustAnotherArchivist/snscrape) for current instructions
2. Change the placeholder text `[REPLACE WITH USERNAME. e.g., 'jack' (not @ sign)]` to the account you want to archive
  If you do **not** want to save retweets, remove `include:nativeretweets`
3. (Optional) You may want to run this script via VPN or proxy 


# Improvements
I currently don't have time to implement this, but I'd like to eventaully create two tables: `retweets` and `replied_to_tweets`. With these tables, the entire chain/thread of conversations can be saved offline, rather than only saving the tweet immediately retweeted/replied to (as is currently the case). This would provide additional context for future generations.

# Why I've Made This
One important aspect of Twitter is it's been a fabulous way of tracking events - when people knew, said or did things. This is important for the historical record, preventing authoritarian gaslighting, holding people accountable, and learning/improving. 

There are also accounts that are just interesting, bring me joy and laughter, etc; and I'd be saddened at being unable to see those again.  


🇺🇦 Слава Україні - Glory to Ukraine
