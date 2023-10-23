# README

Notes: 

Website and Facebook datasets are joined by domain and root_domain, cause after exploring data a little bit it gave me very good results in matching records.

I had much more issues with joining Google dataset with the rest of the data, since only half of facebook/website records could by matched by name from facebook dataset and I couldn't find any better match. I tried to clean name columns a little bit with removal of special characters and trimming, but it didn't help a lot. Given the timeline this is the best I could deliver. 

To make the code work, user needs to create data directory in repository root directory and place 3 datasets there. I was about to make it dynamic and passed as input arguments to main.py script but I didn't find time to do it.