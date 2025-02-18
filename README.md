# Braintrust_data_downloader
Downloads all experiments and datasets from a project on Braintrust into a directory as CSVs

# Requirements
```
requests
python-dotenv
```
```
pip install requirements.txt
```

# How to Run

1. Place your api key into an .env file or into your environment variables
2. Install requirements
3. Use either --project-name or --project-id arguments to specify project and run script
```
python3 main.py --project-name YOUR_PROJECT_NAME
```
```
python3 main.py --project-id YOUR_PROJECT_ID
```

Data is saved to /braintrust_data/experiment or /braintrust_data/dataset

