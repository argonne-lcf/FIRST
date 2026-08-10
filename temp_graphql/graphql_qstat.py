from graphql_common import post_graphql

# Double quotes need to be inside the filter string
# filters = '( filter: {states: [5, 6, 7, 9], jobIds: ["4693"], owner: "msalim"} )'
filters = '( filter: {owner: "msalim", withHistoryJobs: true} )'
# filters = '( filter: {states: [5]} )'
# filters = '( filter: {withHistoryJobs: true, owner: "bcote"} )'
# filters = ''

query = f"""
query {{
    jobs {filters} {{
        edges {{
            node {{
                jobId
                name
                owner
                remoteCommand
                commandArgs
                shellPath
                workDir
                accountingId
                status {{
                    state
                }}
                queue {{
                    name
                }}
                allocatedMachines {{
                    name
                    hostname
                    state
                    port
                    resourcesAvail {{
                        customResources  {{ name value }}
                    }}
                }}
            }}
            error {{
                errorCode
                errorMessage
            }}
        }}
    }}
}}
"""
print(query)

# Send API call to GraphQL
post_graphql(payload={"query": query})
