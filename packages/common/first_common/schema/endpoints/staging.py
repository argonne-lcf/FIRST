from pydantic import BaseModel


class StagingArea(BaseModel):
    """
    A per-user directory on the gateway's Globus data-staging collection, with
    an ACL granting that user read/write access.
    """

    collection_id: str
    path: str
    acl_rule_id: str
    principal: str
