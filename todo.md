1 january - 

  1. Project flow should be ready.. i.e. crawler and sentiment model should be integrated
  2. agent functions should be ready by today.
  3. setup the pathway live table ingestion. and get the data stream in json..
  4. after getting the data in output.jsonl.. with ticker agent can be used to remove noise ( will filter the actual ticker of that particular stock)

2 january - 
 1. agent will alert the dashboard
 2. sentiments to data_stream folder flow should be done.
 3. agent loop should be done

 3 january - 

 1. read lines from output.csv.
 2. let's say I pass 
  {"event_id": "cd3c998ffeae63e63bf61deee84e3b24", "post_id": "1q0jwmq", "event_type": "upsert", "ticker": "UNKNOWN", "text": "Which resources to go for?", "post_des": "I want to read some high quality magazines on macro, investing and markets. I have tried business today for a year, want to switch for this year. Which magazine is best to subscribe to? Give me some suggestions.", "comments": [{"user_name": "AutoModerator", "text": "\nGeneral Guidelines - Buy/Sell, one-liner and Portfolio review posts will be removed.\n\nPlease refer to the [FAQ](https://www.reddit.com/r/IndianStockMarket/wiki/index/) where most common questions have already been answered. Join our Discord server using [this link](https://discord.com/invite/fDRj8mA66U) \n\n\n*I am a bot, and this action was performed automatically. Please [contact the moderators of this subreddit](/message/compose/?to=/r/IndianStockMarket) if you have any questions or concerns.*"}, {"user_name": "Fickle-Childhood747", "text": "Remind me in 6 months!"}], "sentiment": "neutral", "confidence": 0.925, "source": "reddit", "created_at": "2025-12-31T18:28:25Z", "observed_at": "2026-01-02T09:19:39.744727Z", "url": "https://old.reddit.com/r/IndianStockMarket/comments/1q0jwmq/which_resources_to_go_for/", "author": "Zealousideal-Gene272"}

    to llm agent as prompt. it should give me either emitevent or no_alert function as response.

 2. UI should be done
 2. single script to run all environments ( pathway, venv, agent ) everything should be glued together