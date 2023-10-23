# README

Notes: 

Website and Facebook datasets are joined by domain and root_domain, cause after exploring data a little bit it gave me very good results in matching records.

I had much more issues with joining Google dataset with the rest of the data, since only half of facebook/website records could by matched by name from facebook dataset and I couldn't find any better match. I tried to clean name columns a little bit with removal of special characters and trimming, but it didn't help a lot. Given the timeline this is the best I could deliver. 

To make the code work, user needs to create `data` directory in repository root directory and place 3 datasets there. I was about to make it dynamic and passed as input arguments to main.py script but I didn't find time to do it.

It would be good if you install virtual environment first with `python -m venv .venv`, then `.venv/Scripts/activate` for bash or `.venv/Scripts/Activate.ps1` for powershell (Windows)

For Windows Users you might also need some additional configuration for running Spark applications locally, you would need to find `winutils` file for your Spark distribution on github and place it in your local spark installation directory under `{SPARK_HOME}/bin`.

Then install the required packages from `requirements.txt` with `python -m pip install -r requirements.txt`

To run the code you just need Python and simple command execution like `python ./src/main.py`

