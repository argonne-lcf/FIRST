from fastapi import APIRouter, Depends

from first_gateway.apiserver.auth import check_admin, validate_access_token

anon = APIRouter()
auth = APIRouter(dependencies=[Depends(validate_access_token)])
admin = APIRouter(dependencies=[Depends(check_admin)])
