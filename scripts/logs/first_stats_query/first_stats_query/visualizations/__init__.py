"""Registry mapping recipe categories to the functions the CLI can run."""

from collections.abc import Callable

from .helpers import Recipe

_recipes: dict[str, list[tuple[str, Recipe]]] = {}


def recipe(category: str) -> Callable[[Recipe], Recipe]:
    """Register a function as a CLI recipe."""

    def decorator(func: Recipe) -> Recipe:
        _recipes.setdefault(category, []).append((func.__name__, func))
        return func

    return decorator


def get_recipes() -> dict[str, list[tuple[str, Recipe]]]:
    """Return the registered recipes, keyed by category."""
    return _recipes
