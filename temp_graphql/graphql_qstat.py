
from graphql_common import post_graphql

# Double quotes need to be inside the filter string
#filters = '( filter: {states: [5, 6, 7, 9], jobIds: ["7299126"], owner: "bcote"} )'
#filters = '( filter: {states: [5]} )'
filters = '( filter: {withHistoryJobs: true, owner: "bcote"} )'
#filters = ''

query = f"""
query {{
    jobs {filters} {{
        edges {{
            node {{
                jobId
                name
                owner
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

# Send API call to GraphQL
post_graphql(payload = {"query": query})