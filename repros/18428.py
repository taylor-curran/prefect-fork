"""
Reproduction script for GitHub issue #18428
Tests websocket timeout handling when watching long-running flows
"""
import asyncio
from prefect import flow
from prefect.flow_runs import wait_for_flow_run
from prefect.client.orchestration import get_client

@flow
async def long_running_flow():
    """A flow that runs longer than typical websocket timeouts"""
    await asyncio.sleep(300)  # 5 minutes
    return "completed"

async def test_websocket_timeout():
    """Test that websocket timeouts are handled gracefully"""
    async with get_client() as client:
        flow_run = await client.create_flow_run(
            flow=long_running_flow,
            name="test-websocket-timeout"
        )
        
        print(f"Created flow run {flow_run.id}, waiting for completion...")
        
        finished_flow_run = await wait_for_flow_run(
            flow_run_id=flow_run.id,
            timeout=600  # 10 minutes
        )
        
        print(f"Flow run completed with state: {finished_flow_run.state}")

if __name__ == "__main__":
    asyncio.run(test_websocket_timeout())
