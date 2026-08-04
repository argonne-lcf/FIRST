from graphql_common import post_graphql

# PBS job setup
queue = "workq"
nb_nodes = 1
error_path = "/home/bcote"
output_path = "/home/bcote"
compute_allocation = "inference_service"
walltime_sec = 300

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

# Variables to set the number of nodes
SLOTS_PER_NODE = 288
node_index = f"0-{nb_nodes-1}" if nb_nodes > 1 else "0"
task_count = nb_nodes

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
post_graphql(payload = {"query": query})