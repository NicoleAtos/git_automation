# Retrieve variables
tenant_id="$1"
client_id="$2"
client_secret="$3"
subscription_id="$4"
env="$5"

resourceGroup="rg-elsa-${env}"
workspaceName="dbw-elsa-${env}"

# Generate PAT
accessToken=$(curl -X POST https://login.microsoftonline.com/${tenant_id}/oauth2/token \
  -F resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  -F client_id=${client_id} \
  -F grant_type=client_credentials \
  -F client_secret=${client_secret} | jq .access_token --raw-output)

managementToken=$(curl -X POST https://login.microsoftonline.com/${tenant_id}/oauth2/token \
  -F resource=https://management.core.windows.net/ \
  -F client_id=${client_id} \
  -F grant_type=client_credentials \
  -F client_secret=${client_secret} | jq .access_token --raw-output)

workspaceUrl=$(curl -X GET \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $managementToken" \
  https://management.azure.com/subscriptions/${subscription_id}/resourcegroups/$resourceGroup/providers/Microsoft.Databricks/workspaces/$workspaceName?api-version=2018-04-01 \
  | jq .properties.workspaceUrl --raw-output)

# Store as a pipeline variable
echo "##vso[task.setvariable variable=accessToken;issecret=true]$accessToken"
echo "##vso[task.setvariable variable=workspaceUrl]$workspaceUrl"