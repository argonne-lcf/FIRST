## PBS GraphQL on Tara

Add the following to your local `.env` file:
```bash
GRAPHQL_URL=https://graphql-bridge-dev-01.lab.alcf.anl.gov/tara/graphql

KEYCLOAK_REALM=Tara
KEYCLOAK_BASE_URL=https://keycloak.alcf.anl.gov
KEYCLOAK_PUBLIC_CLIENT_ID=ALCF-PBS-PUBLIC
KEYCLOAK_IMPERSONATION_CLIENT_ID=<...>
KEYCLOAK_IMPERSONATION_CLIENT_SECRET=<...>
KEYCLOAK_ACCESS_TOKEN=<...>
KEYCLOAK_AUDIENCE=ALCF-PBS-PUBLIC

SSL_VERIFY=False
```

### Keycloak Token (personal)

```bash
./keycloak_get_personal_tokens.sh
```

Enter your ALCF credentials, and copy-paste the `access_token` value from the response into your `.env` file. 

### Qstat and Qsub

Edit the `graphql_qstat.py` and `graphql_qsub.py` to fit your needs, and execute them with `python`.