# Retrieve variables
accessToken="$1"
workspaceUrl="$2"

# Headers for API calls
authHeader="Authorization: Bearer ${accessToken}"
contentHeader="Content-Type: application/json"

# Fetch existing jobs list ONCE
existingJobs=$(curl -s -X GET "https://${workspaceUrl}/api/2.1/jobs/list?expand_tasks=true" -H "${authHeader}")

# Process each job definition
for jobFile in $(find . -type f -name "*.json"); do
    echo "Processing job definition: ${jobFile}"

    jobName=$(jq -r '.name' "${jobFile}")

    # Find matching job by name
    existingJobId=$(echo "${existingJobs}" | jq -r --arg JOB_NAME "${jobName}" '.jobs[] | select(.settings.name == $JOB_NAME) | .job_id' || true)

    if [[ -z "${existingJobId}" ]]; then
        echo "Job '${jobName}' not found. Creating new job..."

        createResponse=$(curl -s -X POST "https://${workspaceUrl}/api/2.1/jobs/create" \
            -H "${authHeader}" \
            -H "${contentHeader}" \
            -d @"${jobFile}")

        createdJobId=$(echo "${createResponse}" | jq -r '.job_id')
        echo "Created job '${jobName}' with id: ${createdJobId}"

    else
        echo "Job '${jobName}' already exists (job_id: ${existingJobId}). Updating job definition..."

        resetPayload=$(jq -n --argjson new_settings "$(cat "${jobFile}")" \
                            --arg job_id "${existingJobId}" \
                            '{job_id: ($job_id | tonumber), new_settings: $new_settings}')

        curl -s -X POST "https://${workspaceUrl}/api/2.1/jobs/reset" \
            -H "${authHeader}" \
            -H "${contentHeader}" \
            -d "${resetPayload}"

        echo "Updated job_id: ${existingJobId} (${jobName})"
    fi

    echo "---------------------------------------------"
done