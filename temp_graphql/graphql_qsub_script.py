import base64

from graphql_common import post_graphql

# PBS job setup
queue = "workq"
nb_nodes = 1
error_path = "/home/msalim"
output_path = "/home/msalim"
compute_allocation = "inference_service"
walltime_sec = 300

# The full job script. Goes through scriptContent (base64), so no escaping/one-lining
# is needed -- newlines, quotes, and heredocs are all preserved verbatim.
script = """#!/bin/bash
echo "hello"

# Run an embedded python script via a heredoc. Quoting the delimiter ('PYEOF')
# stops the shell from expanding anything inside the python block.
python3 <<'PYEOF'
for i in range(1, 31):
    print(i)
PYEOF
"""

# scriptContent expects urlsafe base64 encoded script content (per schema Base64 type).
script_b64 = base64.urlsafe_b64encode(script.encode()).decode()

# Variables to set the number of nodes
SLOTS_PER_NODE = 288
node_index = f"0-{nb_nodes - 1}" if nb_nodes > 1 else "0"
task_count = nb_nodes

# Build query
query = f"""
mutation {{
    createJob (
        input: {{
            scriptContent: "{script_b64}"
            name: "test"
            resourcesRequested: {{
                jobResources: {{
                    index: ""
                    wallClockTime: {walltime_sec}
                }}
                taskCount: {{
                    min: {task_count}
                    max: {task_count}
                }}
                tasksResources:[
                    {{
                        index: "{node_index}"
                        slots: {SLOTS_PER_NODE}
                    }}
                ]
            }}
            queue: {{
                name: "{queue}"
            }}
            accountingId: "{compute_allocation}"
            errorPath: "{error_path}"
            outputPath: "{output_path}"
        }}
    ){{
        node {{
            jobId
            status {{
                state
            }}
        }}
        error {{
            errorCode
            errorMessage
        }}
    }}
}}
"""

# Send API call to GraphQL
post_graphql(payload={"query": query})
