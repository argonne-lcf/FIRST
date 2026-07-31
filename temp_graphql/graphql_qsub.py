from graphql_common import post_graphql

# PBS job setup
queue = "workq"
error_path = "/home/bcote"
output_path = "/home/bcote"
compute_allocation = "inference_service"
walltime_sec = 300
physicalMemory = 100

# Commands to be executed
commands = """
echo Start
sleep 10
echo "slept for 10 secs"
echo "using following python executable"
which python
echo End
"""

# Format commands to a single line
commands = commands.strip()
commands = "; ".join(line.strip() for line in commands.splitlines() if line.strip())
commands = commands.replace('"', '\\"')

# Build query
query = f"""
mutation {{
    createJob (
        input: {{
            remoteCommand: "/bin/bash"
            commandArgs: ["-lc", "{commands}"]
            name: "test"
            resourcesRequested: {{
                jobResources: {{
                    index: ""
                    wallClockTime: {walltime_sec}
                    physicalMemory: {physicalMemory}
                }}
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
post_graphql(payload = {"query": query})