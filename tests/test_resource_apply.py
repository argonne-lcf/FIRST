import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from first_gateway.database.models import Cluster

from .fixtures.auth import ADMIN_TOKEN, auth_header


async def test_list_existing_clusters(
    client: httpx.AsyncClient, db_session: AsyncSession
) -> None:

    db_session.add(
        Cluster(
            name="sophia",
            status_method="first_gateway.platforms.health.get_alcf_cluster_status",
            status_kwargs={},
            maintenance_notice=None,
            pilot_system=None,
            last_status_check=None,
        )
    )
    await db_session.commit()

    resp = await client.get("/clusters", headers=auth_header(ADMIN_TOKEN))
    assert resp.status_code == 200
    body = resp.json()
    assert [c["name"] for c in body] == ["sophia"]
    assert body[0]["kind"] == "Cluster"
