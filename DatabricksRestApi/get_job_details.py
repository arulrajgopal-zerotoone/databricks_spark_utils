import requests

# Replace these with your values
DATABRICKS_INSTANCE = 'https://adb-1385886232364444.4.azuredatabricks.net' 
TOKEN = "paste_your_token_here"  # Replace with your Databricks token



# Set headers
headers = {
    "Authorization": f"Bearer {TOKEN}"
}



# Check response
job_ids = []
limit = 26
offset = 0
while True:
    # Set up the endpoint
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/list?limit={limit}&offset={offset}"

    response = requests.get(url, headers=headers).json()
    # print(response)
    for job in response['jobs']:
        job_ids.append(job['job_id'])

    if 'next_page_token' in response:
        offset += limit
    else:
        break


print(job_ids)

# Get job details for each job ID
list_job_details = []

for job_id in job_ids:
    # Set up the endpoint
    url = f"{DATABRICKS_INSTANCE}/api/2.1/jobs/get?job_id={job_id}"

    response = requests.get(url, headers=headers).json()
    list_job_details.append(response)

print(list_job_details)
