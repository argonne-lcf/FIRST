"""
Globus data staging: a per-user directory on the staging collection that the
user can transfer data in and out of.
"""

import logging

import anyio
from globus_sdk import (
    ClientCredentialsAuthorizer,
    ConfidentialAppAuthClient,
    TransferClient,
)

from first_common.errors import ServiceUnavailable
from first_common.schema.auth import UserAuthEvent
from first_common.schema.endpoints.staging import StagingArea

from ..settings import ClientState

logger = logging.getLogger(__name__)

_transfer_client: TransferClient | None = None


def get_transfer_client(state: ClientState) -> TransferClient:
    """
    Cache a TransferClient authenticated as the compute service account.

    This must not use `state.auth_client` (the resource-server app), which is
    not permitted the client_credentials grant. As in V1, the service account
    identity is the one granted access-manager rights on the staging collection.
    """
    global _transfer_client
    if _transfer_client is None:
        globus = state.settings.globus
        service_client = ConfidentialAppAuthClient(
            globus.compute_client_id,
            globus.compute_client_secret.get_secret_value(),
        )
        _transfer_client = TransferClient(
            authorizer=ClientCredentialsAuthorizer(
                service_client, TransferClient.scopes.all
            )
        )
    return _transfer_client


def prep_globus_staging_area(
    tc: TransferClient, principal_id: str, collection_id: str
) -> StagingArea:
    """
    Create or refresh ACLs on a per-user staging directory under `collection_id`.

    A directory named with the user's principal ID is (idempotently) created and
    granted read/write ACLs to that user so they can transfer data in and out.
    """
    logger.info(f"User {principal_id=} requesting staging area in {collection_id=}")

    staging_path = f"/user-staging/{principal_id}/"

    try:
        tc.operation_mkdir(collection_id, staging_path)
        logger.info(f"staging directory {staging_path=} created")
    except tc.error_class as e:
        if "exists" not in str(e).lower():
            raise
        logger.info(f"staging directory {staging_path=} already exists")

    existing = tc.endpoint_acl_list(collection_id)
    rule = next(
        (
            r
            for r in existing
            if r["principal"] == principal_id and r["path"] == staging_path
        ),
        None,
    )

    if rule is None:
        result = tc.add_endpoint_acl_rule(
            collection_id,
            dict(
                DATA_TYPE="access",
                principal_type="identity",
                principal=principal_id,
                path=staging_path,
                permissions="rw",
            ),
        )
        acl_rule_id = str(result["access_id"])
        logger.info(f"Granted rw access via {acl_rule_id=}")
    else:
        acl_rule_id = str(rule["id"])
        logger.info(f"Staging area {acl_rule_id=} already exists for {principal_id=}")

    return StagingArea(
        collection_id=collection_id,
        path=staging_path,
        acl_rule_id=acl_rule_id,
        principal=principal_id,
    )


async def ensure_staging_area(user: UserAuthEvent, state: ClientState) -> StagingArea:
    """Idempotently create the caller's Globus staging area."""
    collection_id = state.settings.data_staging_globus_collection_id
    if not collection_id:
        raise ServiceUnavailable(
            "Data staging is not configured on this gateway "
            "(FIRST_DATA_STAGING_GLOBUS_COLLECTION_ID is unset)"
        )
    tc = get_transfer_client(state)
    return await anyio.to_thread.run_sync(
        prep_globus_staging_area,
        tc,
        user.id,
        collection_id,
    )
