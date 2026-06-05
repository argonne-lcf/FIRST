from pydantic import BaseModel


class GlobusStagingAreaPrepared(BaseModel):
    collection_id: str
    path: str
    acl_rule_id: str
    principal: str
