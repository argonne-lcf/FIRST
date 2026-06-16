from fastapi import APIRouter, Depends

from first_gateway.apiserver.routes.admin import router as admin_router

from ..dependencies import get_admin_user, get_auth_user

anon = APIRouter()
auth = APIRouter(dependencies=[Depends(get_auth_user)])
admin = APIRouter(dependencies=[Depends(get_admin_user)])
admin.include_router(admin_router)
