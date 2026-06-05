# Globus Setup

This guide covers creating a Globus project, registering Globus Applications, and optionally creating Globus groups and policies to control access to the service. **Securely store all of your UUIDs and secrets along the way**, you will need them at later stages.

## Step 1: Create Globus Project

A Globus project will store all of the required applications and policies. To create one:

1. Visit [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers)
2. Click on **Add Project** (should be on the top-right corner)
3. Fill the form (e.g. Project Name: Inference)
    - Set **Project Name**: e.g., Inference
    - Set **Contact Email**: Your email
    - Set **Project Admins**: Add at least one of your Globus identity
    - Click on **Submit**

## Step 2: Register Service API Application

This application is at the core of the API's authorization layer. It communicates with the [Globus Auth](https://www.globus.org/globus-auth-service) service to introspect incoming access tokens, and extracts the list of Globus Group memberships of each user. To register the application:

1. Visit [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers)
2. Click on **Register a service API ...**
3. Select your Globus project
4. Fill the form:
   - Set **App Name**: e.g., Inference Service API
   - Set **Redirect URIs**: `http://localhost:8000`
   - You can use the default value for all other fields
   - Click on **Register App**
5. You should now be able to see your application details, including your **Client UUID**
6. Click on **Add Client Secret**
    - Add description (e.g., "inference token introspection")
    - Click on **Generate Secret**

### Add Scope to Service API Application

To allow your Service API application to introspect incoming access tokens, you need a Globus scope that is specifically tied to your inference service. First, export your Service API client credentials into environment variables:
```bash
CLIENT_ID="<Your-Service-API-Client-UUID>"
CLIENT_SECRET="<Your-Service-API-Client-Secret>"
```

Define your scope name (e.g., My Inference Scope) and description (e.g., Access to my inference service):
```bash
SCOPE_NAME="<Your-Scope-Name>"
SCOPE_DESCRIPTION="<Your-Scope-Description>"
```

Execute the following command to create an `action_all` scope to your client. Make sure you keep the `73320ffe-4cb4-4b25-a0a3-83d53d59ce4f` dependent scope, which will allow your Service API client to access the Globus Group memberships of your users from their access tokens.
```bash
curl -X POST -s --user $CLIENT_ID:$CLIENT_SECRET \
    https://auth.globus.org/v2/api/clients/$CLIENT_ID/scopes \
    -H "Content-Type: application/json" \
    -d '{
        "scope": {
            "name": $SCOPE_NAME,
            "description": $SCOPE_DESCRIPTION,
            "scope_suffix": "action_all",
            "dependent_scopes": [
                {
                    "scope": "73320ffe-4cb4-4b25-a0a3-83d53d59ce4f",
                    "optional": false,
                    "requires_refresh_token": true
                }
            ]
        }
    }'
```

Verify that the scope was properly created (look for the `scopes` field in the response):
```bash
curl -s --user $CLIENT_ID:$CLIENT_SECRET https://auth.globus.org/v2/api/clients/$CLIENT_ID
```

Look at the details of your scope:
```bash
SCOPE_ID="<Your-Service-API-Scope-UUID>"
curl -s --user $CLIENT_ID:$CLIENT_SECRET \
    https://auth.globus.org/v2/api/clients/$CLIENT_ID/scopes/$SCOPE_ID
```

Store your scope UUID, you will need it in a later stage.

## Step 3: Register Service Account Application

To handle the communication between the Gateway API and the compute resources (the Inference Backend), you need to create a Globus **Service Account application**. This application represents the Globus identity that will own the [Globus Compute](https://www.globus.org/compute) endpoints.

1. Visit [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers)
2. Click on **Register a service account ...**
3. Select your Globus project
4. Fill the form:
   - Set **App Name**: e.g., My Inference Endpoints
   - Set **Privacy Policy** and **Terms & Conditions** URLs if applicable.
   - Click on **Register App**
5. You should now be able to see your application details, including your **Client UUID**
6. Click on **Add Client Secret**
    - Add description (e.g., "inference compute endpoints")
    - Click on **Generate Secret**

## Step 4: Register Public Auth Client

To provide users with an easy mechanism to get access tokens for the inference service, you can create a public client that will handle all communications with Globus to authenticate users:

1. Visit [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers)
2. Click on **Register a thick client ...**
3. Select your Globus project
4. Fill the form:
   - Set **App Name**: e.g., Public User Auth Client
   - Set **Redirect URIs**: `https://auth.globus.org/v2/web/auth-code`
   - You can use the default value for all other fields
   - Click on **Register App**
5. You should now be able to see your application details, including your **Client UUID**. This type of client has no secret.

## Step 5: Create a Globus High-Assurance Policy

If you want to restrict access to your service based on institution domains, you can enforce a [Globus High Assurance](https://docs.globus.org/guides/overviews/security/high-assurance-overview/#user_authentication_and_access) Policy. This should be used if you only want to authorize specific identity providers (e.g., alcf.anl.gov, your-university.edu, etc.). To create such a policy:

1. Visit [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers)
2. Click on your Globus project
3. Click on the **Policies** tab
4. Click on **Add a Policy**
5. Fill the form:
    - Set **Display Name**: e.g., My Inference High Assurance Policy
    - Set **Description**: e.g., My policy to restrict access
    - [IMPORTANT] Check the **High Assurance** check box
    - Add list of authorized domains in **Included Domains** (one per line)
    - Define for how long you want to allow automated token refreshes
    - Click on **Create Policy**
6. You should now be able to see your policy details, including your **Policy ID**.

## [Optional] Step 6: Create Globus Groups

If you want to further restrict access down to specific users (i.e., specific Globus identities), or if you want to implement role-base access within your inference service, you can create [Globus Groups](https://docs.globus.org/guides/tutorials/manage-identities/manage-groups/). To do so:

1. Visit [https://app.globus.org/groups](https://app.globus.org/groups)
2. Click on **Create new group** (should be on the top-right corner)
3. Fill the form:
    - Set **Group Name** and **Description**
    - Make sure you set the group visibility to your needs
    - Click on **Create Group**
4. You should now be able to see your group overview details, including your **Group UUID**.

To add members to a specific group:
1. Visit [https://app.globus.org/groups](https://app.globus.org/groups)
2. Click on the targetted group
3. Click on **Members** tab
4. Click on **Invite Others**
5. Search for the Globus identity (identity UUID, email, or username)
6. Select appropriate role (typically **Member** for service users)
7. Click on **Send invitation** to send an invitation email to the user

Alternatively, if you have the Globus CLI installed, you can add users directly without sending them an invitation email:
```bash
GROUP_ID="<Your-Globus-Group>"
USER_ID="<User-ID-or-Username-You-Want-To-Add>"
globus group member add $GROUP_ID $USER_ID
```