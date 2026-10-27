import inspect
from typing import Any, Awaitable, Callable, Dict, List, Optional

TaskHandler = Callable[..., Any]


class TaskRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, TaskHandler] = {}

    def register(self, task_name: str) -> Callable[[TaskHandler], TaskHandler]:
        def decorator(func: TaskHandler) -> TaskHandler:
            if task_name in self._registry:
                raise ValueError(f"Task '{task_name}' is already registered.")
            self._registry[task_name] = func
            return func
        return decorator

    def get(self, task_name: str) -> Optional[TaskHandler]:
        return self._registry.get(task_name)

    def is_registered(self, task_name: str) -> bool:
        return task_name in self._registry

    def list_tasks(self) -> List[str]:
        return sorted(list(self._registry.keys()))

    async def execute(self, task_name: str, payload: dict[str, Any], attempt: int = 1) -> Any:
        handler = self.get(task_name)
        if handler is None:
            raise KeyError(f"Task '{task_name}' is not registered.")

        sig = inspect.signature(handler)
        kwargs: dict[str, Any] = {}
        if "payload" in sig.parameters:
            kwargs["payload"] = payload
        elif len(sig.parameters) == 1:
            first_param = next(iter(sig.parameters.values()))
            kwargs[first_param.name] = payload

        if "attempt" in sig.parameters:
            kwargs["attempt"] = attempt

        if inspect.iscoroutinefunction(handler):
            return await handler(**kwargs)
        else:
            return handler(**kwargs)


task_registry = TaskRegistry()
