import globus_compute_sdk


def qstat_inference_function():
    import json
    import os
    import re
    import subprocess

    def run_command(cmd):
        """Run a command and return its output as a list of lines."""
        result = subprocess.run(
            cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {cmd}\n{result.stderr}")
        return result.stdout.strip().split("\n")

    def parse_qstat_xf_output(lines):
        attributes = {}
        current_attr = None
        current_val_lines = []

        # Allow dots and other characters in attribute names.
        attr_line_pattern = re.compile(r"^\s*([A-Za-z0-9_\.\-]+)\s*=\s*(.*)$")

        for line in lines:
            match = attr_line_pattern.match(line)
            if match:
                # Store previous attribute
                if current_attr is not None:
                    attributes[current_attr] = "".join(current_val_lines).strip()
                current_attr = match.group(1)
                current_val = match.group(2)
                current_val_lines = [current_val.strip()]
            else:
                # Continuation line for the current attribute
                if current_attr is not None:
                    current_val_lines.append(line.strip())

        # Store the last attribute
        if current_attr is not None:
            attributes[current_attr] = "".join(current_val_lines).strip()

        return attributes

    def extract_submit_path(submit_args):
        # submit_args should now be a fully restored single line.
        parts = submit_args.split()
        if not parts:
            return None
        return parts[-1]

    def extract_models_info_from_file(file_path, job_dict):
        """
        This function now extracts model_name(s), framework, and cluster from the file.
        Returns a dict with keys: 'models', 'framework', 'cluster'.
        """
        models_str = "N/A"
        framework_str = "N/A"
        cluster_str = "N/A"
        if not os.path.exists(file_path):
            # We'll return a dict with N/A if file doesn't exist
            job_dict["Models"] = models_str
            job_dict["Framework"] = framework_str
            job_dict["Cluster"] = cluster_str
            return job_dict
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        # Extract all model_name= lines
        model_pattern = re.compile(r'model_name\S*\s*=\s*"([^"]+)"')
        all_models = model_pattern.findall(content)
        models_str = ",".join(all_models) if all_models else "N/A"

        # Extract framework=
        framework_pattern = re.compile(r'framework\s*=\s*"([^"]+)"')
        found_framework = framework_pattern.findall(content)
        framework_str = found_framework[0] if found_framework else "N/A"

        # Extract cluster=
        cluster_pattern = re.compile(r'cluster\s*=\s*"([^"]+)"')
        found_cluster = cluster_pattern.findall(content)
        cluster_str = found_cluster[0] if found_cluster else "N/A"

        job_dict["Models"] = models_str
        job_dict["Framework"] = framework_str
        job_dict["Cluster"] = cluster_str
        return job_dict

    def determine_model_status(submit_path, job_dict):
        """
        Determine model_status by checking submit_path + '.stdout' file.
        If file does not exist or line not found, model_status = 'starting'
        If line "All models started successfully." is found, model_status = 'running'
        """
        out_file = submit_path + ".stdout"
        if not os.path.exists(out_file):
            job_dict["Model Status"] = "starting"
            return job_dict

        with open(out_file, "r", encoding="utf-8") as f:
            for line in f:
                if "All models started successfully." in line:
                    job_dict["Model Status"] = "running"
                    return job_dict
        job_dict["Model Status"] = "starting"
        return job_dict

    def determine_batch_job_status(job_id, job_dict):
        home_dir = os.path.expanduser("~")
        batch_jobs_path = os.path.join(home_dir, "batch_jobs")
        # Get all files in the batch_jobs directory, sorted by modification time with the latest file first
        batch_jobs_files = os.listdir(batch_jobs_path)
        batch_jobs_files.sort(
            key=lambda x: os.path.getmtime(os.path.join(batch_jobs_path, x)),
            reverse=True,
        )
        job_dict["Model Status"] = "starting"
        # Check if any file name contains the job id from batch_jobs_files
        for file in batch_jobs_files:
            if job_id in file:
                # split the file name by underscore and fetch model_name, batch_id, username, pbs_job_id
                model_name, batch_id, username, pbs_job_id = file.split("_")
                job_dict["Models"] = model_name
                job_dict["Batch ID"] = batch_id
                job_dict["Username"] = username
                job_dict["Model Status"] = "running"
                return job_dict
        return job_dict

    def common_job_attributes(attributes, job_dict, job_id, job_state):
        job_dict["Job ID"] = job_id
        job_dict["Job State"] = job_state
        job_dict["Host Name"] = attributes.get("exec_host", "N/A")
        job_dict["Job Comments"] = attributes.get("comment", "N/A")
        job_dict["Nodes Reserved"] = attributes.get("Resource_List.nodect", "N/A")
        walltime = attributes.get("resources_used.walltime", "N/A")
        if walltime != "N/A":
            job_dict["Walltime"] = walltime
        estimated_start = attributes.get("estimated.start_time", "N/A")
        if estimated_start != "N/A":
            estimated_start += " (Chicago time)"
            job_dict["Estimated Start Time"] = estimated_start
        return job_dict

    def run_qstat():
        user = os.environ.get("USER")
        if not user:
            raise RuntimeError("USER environment variable not set.")

        # Get extended info for *only* this user's jobs
        qstat_cmd = f"TZ='America/Chicago' qselect -u {user} | xargs -r qstat -xf"
        try:
            full_output = run_command(qstat_cmd)
        except RuntimeError:
            # No jobs for this user
            return {
                "running": [],
                "queued": [],
                "others": [],
                "private-batch-running": [],
                "private-batch-queued": [],
            }

        # Split output into per-job blocks (look for "Job Id:")
        jobs_raw, current = [], []
        for line in full_output:
            if line.startswith("Job Id:"):
                if current:
                    jobs_raw.append(current)
                current = [line]
            else:
                current.append(line)
        if current:
            jobs_raw.append(current)

        # Buckets
        running_jobs, queued_jobs, other_jobs = [], [], []
        private_batch_running, private_batch_queued = [], []

        # Parse each job
        for job_lines in jobs_raw:
            attributes = parse_qstat_xf_output(job_lines)
            job_id = job_lines[0].split()[2]
            job_state = attributes.get("job_state", "N/A")
            job_dict = {}

            submit_path = extract_submit_path(attributes.get("Submit_arguments", ""))
            if submit_path:
                job_dict = extract_models_info_from_file(submit_path, job_dict)

            if job_state == "R":
                if "batch_job" in job_dict.get("Models", ""):
                    job_dict = determine_batch_job_status(job_id, job_dict)
                    job_dict = common_job_attributes(
                        attributes, job_dict, job_id, job_state
                    )
                    private_batch_running.append(job_dict)
                else:
                    job_dict = determine_model_status(submit_path, job_dict)
                    job_dict = common_job_attributes(
                        attributes, job_dict, job_id, job_state
                    )
                    running_jobs.append(job_dict)
            elif job_state == "Q":
                job_dict["Model Status"] = "queued"
                if "batch_job" in job_dict.get("Models", ""):
                    job_dict = common_job_attributes(
                        attributes, job_dict, job_id, job_state
                    )
                    private_batch_queued.append(job_dict)
                else:
                    job_dict = common_job_attributes(
                        attributes, job_dict, job_id, job_state
                    )
                    queued_jobs.append(job_dict)
            else:
                job_dict["Model Status"] = "other"
                job_dict = common_job_attributes(
                    attributes, job_dict, job_id, job_state
                )
                other_jobs.append(job_dict)

        return {
            "running": running_jobs,
            "queued": queued_jobs,
            "others": other_jobs,
            "private-batch-running": private_batch_running,
            "private-batch-queued": private_batch_queued,
        }

    def get_node_status():
        """
        Determines the number of free nodes on the cluster.
        Currently supports PBS via 'pbsnodes'. Add checks for other schedulers here.
        Returns a dictionary like {'free_nodes': count}.
        Returns {'free_nodes': -1} if status cannot be determined.
        """
        free_nodes_count = -1  # Default to unknown
        try:
            # --- PBS Implementation ---
            # Check if pbsnodes command exists
            pbs_check_cmd = "command -v pbsnodes"
            pbs_check_result = subprocess.run(
                pbs_check_cmd,
                shell=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if pbs_check_result.returncode == 0:
                cmd = "pbsnodes -a -F json"
                result = subprocess.run(
                    cmd,
                    shell=True,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=15,
                )
                if result.returncode == 0:
                    try:
                        pbsnodes_data = json.loads(result.stdout)
                        # Count nodes where state is 'free' and not marked as broken
                        count = 0
                        for node_name, node_info in pbsnodes_data.get(
                            "nodes", {}
                        ).items():
                            if (
                                node_info.get("state") == "free"
                                and node_info.get("resources_available", {}).get(
                                    "broken"
                                )
                                != "True"
                            ):
                                count += 1
                        free_nodes_count = count
                    except json.JSONDecodeError as e:
                        print(f"Error parsing pbsnodes JSON: {e}")  # Log error
                    except Exception as e:
                        print(f"Error processing pbsnodes data: {e}")  # Log error
                else:
                    print(f"pbsnodes command failed: {result.stderr}")  # Log error
            else:
                # --- Add Slurm/Other Scheduler Logic Here ---
                # Example placeholder for Slurm:
                # slurm_check_cmd = "command -v sinfo"
                # if subprocess.run(slurm_check_cmd, ...).returncode == 0:
                #    cmd = "sinfo -h -o '%N %t' | grep 'idle' | wc -l"
                #    result = subprocess.run(cmd, ...)
                #    free_nodes_count = int(result.stdout.strip())
                pass  # No other schedulers implemented yet

        except subprocess.TimeoutExpired:
            print("Node status command timed out.")  # Log error
        except Exception as e:
            print(f"Error getting node status: {e}")  # Log error

        return {"free_nodes": free_nodes_count}

    output = run_qstat()
    node_status = get_node_status()
    output["cluster_status"] = node_status  # Add node status to the main output

    json_output = json.dumps(output, indent=4)

    return json_output


# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# # Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(qstat_inference_function)

# # Write function UUID in a file
uuid_file_name = "qstat_register_function_sophia.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# # End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")
