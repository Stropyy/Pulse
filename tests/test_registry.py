import pytest
from shared.registry import TaskRegistry


@pytest.mark.asyncio
async def test_task_registry_register_and_execute():
    registry = TaskRegistry()

    @registry.register("sample_sync")
    def sync_task(payload: dict) -> dict:
        return {"sync_result": payload.get("x", 0) * 2}

    @registry.register("sample_async")
    async def async_task(payload: dict, attempt: int = 1) -> dict:
        return {"async_result": payload.get("y", 0) + 10, "attempt": attempt}

    assert registry.is_registered("sample_sync")
    assert registry.is_registered("sample_async")
    assert not registry.is_registered("unknown_task")

    res_sync = await registry.execute("sample_sync", {"x": 21})
    assert res_sync == {"sync_result": 42}

    res_async = await registry.execute("sample_async", {"y": 32}, attempt=2)
    assert res_async == {"async_result": 42, "attempt": 2}


def test_task_registry_duplicate_registration():
    registry = TaskRegistry()

    @registry.register("duplicate_task")
    def task_one(payload: dict):
        return 1

    with pytest.raises(ValueError, match="already registered"):
        @registry.register("duplicate_task")
        def task_two(payload: dict):
            return 2


@pytest.mark.asyncio
async def test_task_registry_unknown_task():
    registry = TaskRegistry()
    with pytest.raises(KeyError, match="not registered"):
        await registry.execute("non_existent_task", {})
