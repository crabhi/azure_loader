# Load data to Azure

## Input

A tab-separated file with the following columns

- bucket
- key

The Key should be URL-encoded.

## Data mapping

Since the Azure container names can't contain dots, the dots in
the S3 bucket name are mapped to dashes (-) in the Azure container
name.

The keys aren't manipulated.


## Usage

First, set up AWS and Azure credentials. You can use for example `az login` and
`aws sso configure` or `aws configure`. The loader should pick up the credentials
from the environment.

⚠️ In the `Azure storage account > Access Control (IAM) > Role assignments`, you have
to be assigned the role "Storage Blob Data Contributor". **The Owner role is not enough!**

    az login
    aws sso configure
    export AWS_PROFILE=...

    printf 'mybucket\tmyfile.txt\n' | go run main.go  -j 60 -azure-url https://$ACCOUNT_NAME.blob.core.windows.net/
