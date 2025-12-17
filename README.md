## Py Expense Updater

Most of this was coded with AI but it has been tested and it works as expected.
This is a tool used to use my bank CSV files to update my google sheets budget and automatically pull data over.

I am using the [Measure of a Plan](https://themeasureofaplan.com/) budget tracking spreadsheet as a template

### Steps to run

1. Create an `uploads/` folder and add your expense CSV files

2. Generate a Google Sheets service account JSON key and name it `key.json`

3. Install dependencies  
   `pip install -r requirements.txt`

4. Run the script  
   `python script.py`
