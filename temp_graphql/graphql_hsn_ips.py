import json
import sys

import requests
from graphql_common import GRAPHQL_URL, SSL_VERIFY, headers

# Running state per schema (JobStatus.state): 7 = Running
RUNNING_STATE = 7
HSN_RESOURCE_NAME = "hsn_ips"


def run_query(query):
    """Send a GraphQL query and return the parsed JSON response."""
    resp = requests.post(
        GRAPHQL_URL, json={"query": query}, headers=headers, verify=SSL_VERIFY
    )
    return resp.json()


def hsn_ips_from_machine(machine):
    """Pull the hsn_ips custom resource off a Machine node -> list of IPs (may be empty)."""
    resources_avail = machine.get("resourcesAvail") or {}
    for pair in resources_avail.get("customResources") or []:
        if pair.get("name") == HSN_RESOURCE_NAME:
            # value is a single string; split on commas/whitespace for multiple HSN NICs
            return [ip for ip in pair["value"].replace(",", " ").split()]
    return []


def get_job(job_id):
    """Step 1: fetch the job, its state, and its allocated vnodes (with hsn_ips if present)."""
    query = f"""
    query {{
        jobs ( filter: {{jobIds: ["{job_id}"], withHistoryJobs: true}} ) {{
            edges {{
                node {{
                    jobId
                    status {{ state }}
                    allocatedMachines {{
                        name
                        hostname
                        resourcesAvail {{
                            customResources {{ name value }}
                        }}
                    }}
                }}
                error {{ errorCode errorMessage }}
            }}
        }}
    }}
    """
    data = run_query(query)
    edges = data.get("data", {}).get("jobs", {}).get("edges") or []
    if not edges:
        raise SystemExit(
            f"No job found for id {job_id!r}. Response:\n{json.dumps(data, indent=2)}"
        )

    edge = edges[0]
    if edge.get("error"):
        raise SystemExit(f"Job error: {edge['error']}")
    return edge["node"]


def get_vnode_hsn_ips(vnode_names):
    """Step 2 fallback: query the vnodes directly and correlate hsn_ips by vnode name."""
    names = ", ".join(f'"{n}"' for n in vnode_names)
    query = f"""
    query {{
        machines ( filter: {{names: [{names}]}} ) {{
            edges {{
                node {{
                    name
                    hostname
                    resourcesAvail {{
                        customResources {{ name value }}
                    }}
                }}
            }}
        }}
    }}
    """
    data = run_query(query)
    edges = data.get("data", {}).get("machines", {}).get("edges") or []
    return {e["node"]["name"]: hsn_ips_from_machine(e["node"]) for e in edges}


def main(job_id):
    job = get_job(job_id)
    state = (job.get("status") or {}).get("state")
    machines = job.get("allocatedMachines") or []

    if state != RUNNING_STATE:
        print(
            f"WARNING: job {job_id} state is {state}, not Running ({RUNNING_STATE}). "
            f"hsn_ips is only known-good while running.\n"
        )

    if not machines:
        raise SystemExit(
            f"Job {job_id} has no allocated machines (not scheduled / not running yet)."
        )

    # Step 1: try to read hsn_ips straight from the job's allocated vnodes.
    ips_by_vnode = {m["name"]: hsn_ips_from_machine(m) for m in machines}

    # Step 2: if none of them carried the custom resource, query the vnodes directly.
    if not any(ips_by_vnode.values()):
        print(
            "hsn_ips not present on job.allocatedMachines; falling back to machines query...\n"
        )
        ips_by_vnode = get_vnode_hsn_ips(list(ips_by_vnode.keys()))

    # Head node = primary execution host = first vnode in the allocation.
    head = machines[0]
    head_name = head["name"]

    print(f"Job {job_id}  (state={state})")
    print(
        f"Head node (primary execution host): {head_name}  hostname={head.get('hostname')}"
    )
    print(f"Head node hsn_ips: {ips_by_vnode.get(head_name) or '(none found)'}\n")

    print("All allocated vnodes (in allocation order):")
    for i, m in enumerate(machines):
        tag = "  <-- head" if i == 0 else ""
        print(
            f"  {m['name']}  hostname={m.get('hostname')}  hsn_ips={ips_by_vnode.get(m['name'])}{tag}"
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"Usage: python {sys.argv[0]} <job_id>")
    main(sys.argv[1])
